package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed web/*
var webFS embed.FS

var startTime = time.Now()

type server struct{}

func (server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case p == "/":
		http.Redirect(w, r, "/panel", http.StatusFound)
	case p == "/login":
		handleLogin(w, r)
	case p == "/logout":
		dropSession(r)
		http.SetCookie(w, &http.Cookie{Name: sessCookie, Value: "", Path: "/", MaxAge: -1})
		http.Redirect(w, r, "/login", http.StatusFound)
	case p == "/panel":
		if !validSession(r) {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		serveAsset(w, "web/panel.html")
	case strings.HasPrefix(p, "/api/"):
		if !validSession(r) {
			jsonErr(w, 401, "neautentificat")
			return
		}
		handleAPI(w, r)
	case strings.HasPrefix(p, "/udp/"), strings.HasPrefix(p, "/rtp/"):
		handleUdpxy(w, r)
	case strings.HasPrefix(p, "/hls/"):
		handleHLSSegment(w, r)
	case p == "/playlist.m3u" || p == "/playlist.m3u8":
		handlePlaylist(w, r)
	case p == "/favicon.ico":
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write([]byte(faviconSVG))
	case p == "/status":
		http.Redirect(w, r, "/panel", http.StatusFound)
	default:
		handleChannel(w, r)
	}
}

const faviconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#0ea5a4"/><path d="M8 10h10a5 5 0 010 10h-6v4H8zm4 4v2h6a1 1 0 000-2z" fill="#fff"/><circle cx="24" cy="23" r="3" fill="#fde047"/></svg>`

func serveAsset(w http.ResponseWriter, name string) {
	b, err := webFS.ReadFile(name)
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Write(b)
}

func jsonOut(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ---------------- login ----------------

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		if validSession(r) {
			http.Redirect(w, r, "/panel", http.StatusFound)
			return
		}
		serveAsset(w, "web/login.html")
		return
	}
	if r.Method != "POST" {
		http.Error(w, "method", 405)
		return
	}
	ip := clientIP(r)
	if loginLocked(ip) {
		http.Redirect(w, r, "/login?e=locked", http.StatusFound)
		return
	}
	r.ParseForm()
	u, pw := r.PostFormValue("username"), r.PostFormValue("password")
	c := getConfig()
	if u != c.Username || !checkPassword(c.PasswordHash, pw) {
		loginFailed(ip)
		log.Printf("[auth] login esuat pentru %q de la %s", u, ip)
		time.Sleep(700 * time.Millisecond)
		http.Redirect(w, r, "/login?e=bad", http.StatusFound)
		return
	}
	loginOK(ip)
	tok := newSession(u)
	http.SetCookie(w, &http.Cookie{Name: sessCookie, Value: tok, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteStrictMode, MaxAge: 7 * 24 * 3600, Secure: r.TLS != nil})
	log.Printf("[auth] login %s de la %s", u, ip)
	http.Redirect(w, r, "/panel", http.StatusFound)
}

// ---------------- streaming ----------------

func serveTS(w http.ResponseWriter, r *http.Request, src *Source, kind, target string) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	sub := src.Subscribe()
	defer src.Unsubscribe(sub)
	c := clients.Add(clientIP(r), kind, target, r.UserAgent(), cancel)
	c.dropped = sub.dropped.Load
	defer clients.Remove(c)
	log.Printf("[client] +%s %s %s", c.IP, kind, target)
	defer log.Printf("[client] -%s %s %s", c.IP, kind, target)

	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(200)
	rc := http.NewResponseController(w)
	rc.Flush()
	buf := make([]byte, 0, 256*1024)
	idle := time.NewTimer(60 * time.Second)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-idle.C:
			return // no data for 60s
		case d := <-sub.ch:
			buf = append(buf[:0], d...)
		drain:
			for len(buf) < 200*1024 {
				select {
				case d2 := <-sub.ch:
					buf = append(buf, d2...)
				default:
					break drain
				}
			}
			rc.SetWriteDeadline(time.Now().Add(20 * time.Second))
			n, err := w.Write(buf)
			c.bytes.Add(uint64(n))
			if err != nil {
				return
			}
			rc.Flush()
			idle.Reset(60 * time.Second)
		}
	}
}

func handleUdpxy(w http.ResponseWriter, r *http.Request) {
	c := getConfig()
	if !c.UdpxyEnabled {
		http.Error(w, "udpxy dezactivat", 403)
		return
	}
	if !streamAllowed(r) {
		http.Error(w, "acces interzis", 403)
		return
	}
	path := strings.TrimSuffix(r.URL.Path, "/")
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
	if len(parts) != 2 {
		http.Error(w, "format: /udp/239.1.1.1:1234", 400)
		return
	}
	addr := strings.TrimPrefix(parts[1], "@")
	host, port, err := net.SplitHostPort(addr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || ip.To4() == nil || !ip.IsMulticast() {
		http.Error(w, "adresa multicast invalida (ex: /udp/239.1.1.1:1234)", 400)
		return
	}
	pn, err := strconv.Atoi(port)
	if err != nil || pn < 1 || pn > 65535 {
		http.Error(w, "port invalid", 400)
		return
	}
	input := parts[0] + "://"
	if c.Interface != "" {
		input += c.Interface + "@"
	}
	input += ip.String() + ":" + strconv.Itoa(pn)
	src, err := hub.Acquire(input)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer hub.Release(src)
	serveTS(w, r, src, "udpxy", ip.String()+":"+strconv.Itoa(pn))
}

func handleChannel(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	format := "ts"
	switch {
	case strings.HasSuffix(p, "/index.m3u8"):
		p, format = strings.TrimSuffix(p, "/index.m3u8"), "hls"
	case strings.HasSuffix(p, ".m3u8"):
		p, format = strings.TrimSuffix(p, ".m3u8"), "hls"
	case strings.HasSuffix(p, ".ts"):
		p = strings.TrimSuffix(p, ".ts")
	}
	p = strings.TrimSuffix(p, "/")
	ch, ok := findChannel(p)
	if !ok || !ch.Enabled {
		http.NotFound(w, r)
		return
	}
	if !streamAllowed(r) {
		http.Error(w, "acces interzis", 403)
		return
	}
	if format == "hls" {
		seg, err := hlsm.Get(ch)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		tok := ""
		if t := r.URL.Query().Get("token"); t != "" {
			tok = t
		}
		pl, ok := seg.Playlist(tok)
		if !ok {
			http.Error(w, "fara semnal", 503)
			return
		}
		clients.TouchHLS(clientIP(r), ch.Name, r.UserAgent(), 0)
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(pl))
		return
	}
	src, err := hub.Acquire(ch.Input)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer hub.Release(src)
	serveTS(w, r, src, "http-ts", ch.Name)
}

func handleHLSSegment(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/hls/"), "/")
	if len(parts) != 2 || !strings.HasSuffix(parts[1], ".ts") {
		http.NotFound(w, r)
		return
	}
	if !streamAllowed(r) {
		http.Error(w, "acces interzis", 403)
		return
	}
	seq, err := strconv.ParseUint(strings.TrimSuffix(parts[1], ".ts"), 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	ch, ok := findChannel(parts[0])
	if !ok || !ch.Enabled {
		http.NotFound(w, r)
		return
	}
	seg, err := hlsm.Get(ch)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	data := seg.Segment(seq)
	if data == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Access-Control-Allow-Origin", "*")
	n, _ := w.Write(data)
	clients.TouchHLS(clientIP(r), ch.Name, r.UserAgent(), n)
}

func baseURL(r *http.Request) string {
	c := getConfig()
	if c.PublicURL != "" {
		return strings.TrimSuffix(c.PublicURL, "/")
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func handlePlaylist(w http.ResponseWriter, r *http.Request) {
	if !streamAllowed(r) {
		http.Error(w, "acces interzis", 403)
		return
	}
	c := getConfig()
	base := baseURL(r)
	q := ""
	if c.StreamToken != "" {
		q = "?token=" + c.StreamToken
	}
	hls := r.URL.Query().Get("type") == "hls"
	var sb strings.Builder
	sb.WriteString("#EXTM3U\n")
	for _, ch := range c.Channels {
		if !ch.Enabled {
			continue
		}
		title := ch.Title
		if title == "" {
			title = ch.Name
		}
		u := base + "/" + ch.Name
		if hls {
			u += ".m3u8"
		}
		fmt.Fprintf(&sb, "#EXTINF:-1 tvg-id=\"%s\" tvg-name=\"%s\",%s\n%s%s\n", ch.Name, ch.Name, title, u, q)
	}
	w.Header().Set("Content-Type", "audio/x-mpegurl; charset=utf-8")
	w.Header().Set("Content-Disposition", `inline; filename="proxyhub.m3u"`)
	w.Write([]byte(sb.String()))
}

// ---------------- API ----------------

type chanStatus struct {
	Channel
	Status  string `json:"status"`
	Bitrate uint64 `json:"bitrate"`
	Clients int    `json:"clients"`
	HLS     bool   `json:"hls_active"`
	Error   string `json:"error"`
}

type groupStatus struct {
	Input   string `json:"input"`
	Status  string `json:"status"`
	Bitrate uint64 `json:"bitrate"`
	Subs    int    `json:"subs"`
	Error   string `json:"error"`
}

func handleAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" && !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		jsonErr(w, 415, "content-type json necesar")
		return
	}
	switch r.URL.Path {
	case "/api/status":
		apiStatus(w)
	case "/api/config":
		c := getConfig()
		c.PasswordHash = ""
		jsonOut(w, map[string]any{"config": c, "default_password": checkPassword(getConfig().PasswordHash, "admin"),
			"version": version, "os": runtime.GOOS + "/" + runtime.GOARCH})
	case "/api/interfaces":
		apiInterfaces(w)
	case "/api/logs":
		jsonOut(w, logRing.Lines())
	case "/api/channel/save":
		apiChannelSave(w, r)
	case "/api/channel/delete":
		apiChannelDelete(w, r)
	case "/api/channel/toggle":
		apiChannelToggle(w, r)
	case "/api/import":
		apiImport(w, r)
	case "/api/settings":
		apiSettings(w, r)
	case "/api/password":
		apiPassword(w, r)
	case "/api/kick":
		var req struct{ ID uint64 }
		json.NewDecoder(r.Body).Decode(&req)
		jsonOut(w, map[string]bool{"ok": clients.Kick(req.ID)})
	case "/api/restart":
		jsonOut(w, map[string]bool{"ok": true})
		go func() { time.Sleep(300 * time.Millisecond); restartHTTP() }()
	default:
		jsonErr(w, 404, "necunoscut")
	}
}

func apiStatus(w http.ResponseWriter) {
	c := getConfig()
	used := map[string]bool{}
	var chs []chanStatus
	var totalIn uint64
	for _, ch := range c.Channels {
		cs := chanStatus{Channel: ch, Status: "idle"}
		if !ch.Enabled {
			cs.Status = "off"
		}
		if s := hub.Get(ch.Input); s != nil {
			used[ch.Input] = true
			cs.Status = s.Status()
			cs.Bitrate = s.rate.Load()
			cs.Error = s.Err()
		}
		if e := channelErr(ch.Name); e != "" {
			cs.Status, cs.Error = "error", e
		}
		cs.Clients = clients.CountFor("", ch.Name)
		cs.HLS = hlsm.Active(ch.Name)
		chs = append(chs, cs)
	}
	var groups []groupStatus
	for _, s := range hub.All() {
		totalIn += s.rate.Load()
		if used[s.input] {
			continue
		}
		groups = append(groups, groupStatus{Input: s.input, Status: s.Status(), Bitrate: s.rate.Load(), Subs: s.SubCount(), Error: s.Err()})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Input < groups[j].Input })
	cl := clients.List()
	var totalOut uint64
	for _, x := range cl {
		totalOut += x.Rate
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	jsonOut(w, map[string]any{
		"uptime": int64(time.Since(startTime).Seconds()), "version": version,
		"channels": chs, "groups": groups, "clients": cl,
		"total_in": totalIn, "total_out": totalOut, "mem": ms.Alloc, "goroutines": runtime.NumGoroutine(),
		"udpxy": c.UdpxyEnabled, "listen": c.Listen, "token": c.StreamToken,
	})
}

func apiInterfaces(w http.ResponseWriter) {
	type ifc struct {
		Name string   `json:"name"`
		IPs  []string `json:"ips"`
		Up   bool     `json:"up"`
	}
	var out []ifc
	ifs, _ := net.Interfaces()
	for _, i := range ifs {
		x := ifc{Name: i.Name, Up: i.Flags&net.FlagUp != 0}
		addrs, _ := i.Addrs()
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil {
				x.IPs = append(x.IPs, n.IP.String())
			}
		}
		if len(x.IPs) > 0 {
			out = append(out, x)
		}
	}
	jsonOut(w, out)
}

func validateChannel(ch *Channel) error {
	ch.Name = strings.TrimSpace(ch.Name)
	ch.Input = strings.TrimSpace(ch.Input)
	ch.Title = strings.TrimSpace(ch.Title)
	if !nameRe.MatchString(ch.Name) {
		return fmt.Errorf("nume invalid (litere, cifre, _ . -)")
	}
	if reservedNames[strings.ToLower(ch.Name)] {
		return fmt.Errorf("nume rezervat")
	}
	low := strings.ToLower(ch.Input)
	switch {
	case strings.HasPrefix(low, "udp://"), strings.HasPrefix(low, "rtp://"):
		if _, _, _, err := parseUDPInput(ch.Input); err != nil {
			return err
		}
	case strings.HasPrefix(low, "http://"), strings.HasPrefix(low, "https://"):
	default:
		return fmt.Errorf("input trebuie sa inceapa cu udp://, rtp://, http:// sau https://")
	}
	return nil
}

func apiChannelSave(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Channel
		OldName string `json:"old_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	ch := req.Channel
	if err := validateChannel(&ch); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	cfgMu.Lock()
	idx := -1
	for i, c := range cfg.Channels {
		if req.OldName != "" && strings.EqualFold(c.Name, req.OldName) {
			idx = i
		} else if strings.EqualFold(c.Name, ch.Name) {
			cfgMu.Unlock()
			jsonErr(w, 400, "exista deja un canal cu acest nume")
			return
		}
	}
	if idx >= 0 {
		cfg.Channels[idx] = ch
	} else {
		cfg.Channels = append(cfg.Channels, ch)
	}
	err := saveConfigLocked()
	cfgMu.Unlock()
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	if req.OldName != "" && !strings.EqualFold(req.OldName, ch.Name) {
		hlsm.Drop(req.OldName)
	}
	applyChannels()
	log.Printf("[panel] canal salvat: %s -> %s", ch.Name, ch.Input)
	jsonOut(w, map[string]bool{"ok": true})
}

func apiChannelDelete(w http.ResponseWriter, r *http.Request) {
	var req struct{ Name string }
	json.NewDecoder(r.Body).Decode(&req)
	cfgMu.Lock()
	out := cfg.Channels[:0]
	for _, c := range cfg.Channels {
		if !strings.EqualFold(c.Name, req.Name) {
			out = append(out, c)
		}
	}
	cfg.Channels = out
	err := saveConfigLocked()
	cfgMu.Unlock()
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	applyChannels()
	log.Printf("[panel] canal sters: %s", req.Name)
	jsonOut(w, map[string]bool{"ok": true})
}

func apiChannelToggle(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string
		Enabled bool
	}
	json.NewDecoder(r.Body).Decode(&req)
	cfgMu.Lock()
	for i, c := range cfg.Channels {
		if strings.EqualFold(c.Name, req.Name) {
			cfg.Channels[i].Enabled = req.Enabled
		}
	}
	err := saveConfigLocked()
	cfgMu.Unlock()
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	applyChannels()
	jsonOut(w, map[string]bool{"ok": true})
}

var nonName = regexp.MustCompile(`[^A-Za-z0-9_.\-]+`)

func apiImport(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Text     string `json:"text"`
		OnDemand bool   `json:"on_demand"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	lines := strings.Split(strings.ReplaceAll(req.Text, "\r", ""), "\n")
	added, skipped := 0, 0
	title := ""
	cfgMu.Lock()
	exists := map[string]bool{}
	for _, c := range cfg.Channels {
		exists[strings.ToLower(c.Name)] = true
	}
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "#EXTINF") {
			if i := strings.LastIndex(l, ","); i >= 0 {
				title = strings.TrimSpace(l[i+1:])
			}
			continue
		}
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		t := title
		title = ""
		if t == "" {
			t = fmt.Sprintf("ch%d", len(cfg.Channels)+1)
		}
		name := strings.Trim(nonName.ReplaceAllString(t, "_"), "_")
		if name == "" {
			name = fmt.Sprintf("ch%d", len(cfg.Channels)+1)
		}
		if len(name) > 64 {
			name = name[:64]
		}
		base := name
		for k := 2; exists[strings.ToLower(name)] || reservedNames[strings.ToLower(name)]; k++ {
			name = fmt.Sprintf("%s_%d", base, k)
		}
		ch := Channel{Name: name, Title: t, Input: l, Enabled: true, OnDemand: req.OnDemand}
		if validateChannel(&ch) != nil {
			skipped++
			continue
		}
		exists[strings.ToLower(name)] = true
		cfg.Channels = append(cfg.Channels, ch)
		added++
	}
	err := saveConfigLocked()
	cfgMu.Unlock()
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	applyChannels()
	log.Printf("[panel] import M3U: %d adaugate, %d ignorate", added, skipped)
	jsonOut(w, map[string]int{"added": added, "skipped": skipped})
}

func apiSettings(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Listen       string   `json:"listen"`
		Interface    string   `json:"interface"`
		UdpxyEnabled bool     `json:"udpxy_enabled"`
		StreamToken  string   `json:"stream_token"`
		AllowedIPs   []string `json:"allowed_ips"`
		HLSSegment   int      `json:"hls_segment"`
		HLSWindow    int      `json:"hls_window"`
		PublicURL    string   `json:"public_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	req.Listen = strings.TrimSpace(req.Listen)
	if req.Listen == "" {
		req.Listen = ":8090"
	}
	if !strings.Contains(req.Listen, ":") {
		req.Listen = ":" + req.Listen
	}
	if _, _, err := net.SplitHostPort(req.Listen); err != nil {
		jsonErr(w, 400, "adresa de ascultare invalida")
		return
	}
	if req.Interface != "" {
		if _, err := resolveIfaceIP(req.Interface); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
	}
	if req.HLSSegment < 1 || req.HLSSegment > 20 {
		req.HLSSegment = 4
	}
	if req.HLSWindow < 3 || req.HLSWindow > 30 {
		req.HLSWindow = 6
	}
	var ips []string
	for _, a := range req.AllowedIPs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if strings.Contains(a, "/") {
			if _, _, err := net.ParseCIDR(a); err != nil {
				jsonErr(w, 400, "CIDR invalid: "+a)
				return
			}
		} else if net.ParseIP(a) == nil {
			jsonErr(w, 400, "IP invalid: "+a)
			return
		}
		ips = append(ips, a)
	}
	cfgMu.Lock()
	listenChanged := cfg.Listen != req.Listen
	ifChanged := cfg.Interface != req.Interface
	cfg.Listen = req.Listen
	cfg.Interface = strings.TrimSpace(req.Interface)
	cfg.UdpxyEnabled = req.UdpxyEnabled
	cfg.StreamToken = strings.TrimSpace(req.StreamToken)
	cfg.AllowedIPs = ips
	cfg.HLSSegment = req.HLSSegment
	cfg.HLSWindow = req.HLSWindow
	cfg.PublicURL = strings.TrimSpace(req.PublicURL)
	err := saveConfigLocked()
	cfgMu.Unlock()
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	if ifChanged {
		// re-create always-on multicast sources on the new interface
		heldMu.Lock()
		for k, h := range held {
			hub.Release(h.src)
			delete(held, k)
		}
		heldMu.Unlock()
		go func() { time.Sleep(5 * time.Second); applyChannels() }()
	}
	log.Printf("[panel] setari salvate")
	jsonOut(w, map[string]any{"ok": true, "restart_needed": listenChanged})
}

func apiPassword(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Current  string `json:"current"`
		Username string `json:"username"`
		New      string `json:"new"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	req.Username = strings.TrimSpace(req.Username)
	cfgMu.Lock()
	defer cfgMu.Unlock()
	if !checkPassword(cfg.PasswordHash, req.Current) {
		jsonErr(w, 403, "parola curenta gresita")
		return
	}
	if req.Username == "" {
		req.Username = cfg.Username
	}
	if req.New != "" {
		if len(req.New) < 6 {
			jsonErr(w, 400, "parola noua trebuie sa aiba minim 6 caractere")
			return
		}
		cfg.PasswordHash = hashPassword(req.New)
	}
	cfg.Username = req.Username
	if err := saveConfigLocked(); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	log.Printf("[panel] credentiale actualizate (user %s)", req.Username)
	jsonOut(w, map[string]bool{"ok": true})
}
