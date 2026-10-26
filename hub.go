package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- Source: one input, many subscribers ----------

type Sub struct {
	ch      chan []byte
	dropped atomic.Uint64
}

type Source struct {
	key      string
	input    string
	mu       sync.Mutex
	subs     map[*Sub]struct{}
	refs     int
	bytesIn  atomic.Uint64
	rate     atomic.Uint64 // bit/s
	lastData atomic.Int64
	lastErr  atomic.Value
	started  time.Time
	cancel   context.CancelFunc
	stopT    *time.Timer
	prevB    uint64
}

func (s *Source) Subscribe() *Sub {
	sub := &Sub{ch: make(chan []byte, 2048)}
	s.mu.Lock()
	s.subs[sub] = struct{}{}
	s.mu.Unlock()
	return sub
}

func (s *Source) Unsubscribe(sub *Sub) {
	s.mu.Lock()
	delete(s.subs, sub)
	s.mu.Unlock()
}

func (s *Source) SubCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

func (s *Source) publish(b []byte) {
	if len(b) == 0 {
		return
	}
	s.bytesIn.Add(uint64(len(b)))
	s.lastData.Store(time.Now().UnixNano())
	s.mu.Lock()
	for sub := range s.subs {
		select {
		case sub.ch <- b:
		default:
			sub.dropped.Add(1)
		}
	}
	s.mu.Unlock()
}

func (s *Source) setErr(e string) {
	s.lastErr.Store(e)
	if e != "" {
		log.Printf("[src %s] %s", s.input, e)
	}
}

func (s *Source) Err() string {
	v, _ := s.lastErr.Load().(string)
	return v
}

func (s *Source) Status() string {
	ld := s.lastData.Load()
	if ld != 0 && time.Since(time.Unix(0, ld)) < 3*time.Second {
		return "ok"
	}
	if time.Since(s.started) < 4*time.Second {
		return "starting"
	}
	return "nosignal"
}

// ---------- Hub ----------

type Hub struct {
	mu   sync.Mutex
	srcs map[string]*Source
}

var hub = &Hub{srcs: map[string]*Source{}}

func (h *Hub) Acquire(input string) (*Source, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.srcs[input]; s != nil {
		s.refs++
		if s.stopT != nil {
			s.stopT.Stop()
			s.stopT = nil
		}
		return s, nil
	}
	s := &Source{key: input, input: input, subs: map[*Sub]struct{}{}, refs: 1, started: time.Now()}
	s.lastErr.Store("")
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	if err := startInput(ctx, s); err != nil {
		cancel()
		return nil, err
	}
	h.srcs[input] = s
	log.Printf("[hub] start %s", input)
	return s, nil
}

func (h *Hub) Release(s *Source) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s.refs--
	if s.refs > 0 {
		return
	}
	s.stopT = time.AfterFunc(4*time.Second, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if s.refs > 0 || h.srcs[s.key] != s {
			return
		}
		delete(h.srcs, s.key)
		s.cancel()
		log.Printf("[hub] stop %s", s.input)
	})
}

func (h *Hub) Get(input string) *Source {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.srcs[input]
}

func (h *Hub) All() []*Source {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*Source, 0, len(h.srcs))
	for _, s := range h.srcs {
		out = append(out, s)
	}
	return out
}

func (h *Hub) statsLoop() {
	t := time.NewTicker(time.Second)
	for range t.C {
		for _, s := range h.All() {
			b := s.bytesIn.Load()
			s.rate.Store((b - s.prevB) * 8)
			s.prevB = b
		}
	}
}

// ---------- inputs ----------

// parseUDPInput accepts udp://[iface@]group:port, udp://@group:port, rtp://...
func parseUDPInput(raw string) (ip net.IP, port int, iface string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, 0, "", err
	}
	hostport := u.Host
	if u.User != nil { // udp://eth0@239.1.1.1:1234 is parsed as userinfo
		iface = u.User.Username()
	}
	hostport = strings.TrimPrefix(hostport, "@")
	if q := u.Query().Get("iface"); q != "" {
		iface = q
	}
	host, ps, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil, 0, "", fmt.Errorf("adresa invalida %q", hostport)
	}
	port, err = strconv.Atoi(ps)
	if err != nil || port < 1 || port > 65535 {
		return nil, 0, "", fmt.Errorf("port invalid")
	}
	if host == "" {
		host = "0.0.0.0"
	}
	ip = net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		return nil, 0, "", fmt.Errorf("IP invalid %q", host)
	}
	return ip, port, iface, nil
}

func startInput(ctx context.Context, s *Source) error {
	low := strings.ToLower(s.input)
	switch {
	case strings.HasPrefix(low, "udp://"), strings.HasPrefix(low, "rtp://"):
		ip, port, iface, err := parseUDPInput(s.input)
		if err != nil {
			return err
		}
		if iface == "" {
			iface = getConfig().Interface
		}
		unreg, err := mcast.Register(ip, port, iface, func(b []byte) {
			p := stripRTP(b)
			if len(p) == 0 {
				return
			}
			c := make([]byte, len(p))
			copy(c, p)
			s.publish(c)
		})
		if err != nil {
			return err
		}
		go func() { <-ctx.Done(); unreg() }()
		return nil
	case strings.HasPrefix(low, "http://"), strings.HasPrefix(low, "https://"):
		if strings.Contains(low, ".m3u8") {
			go hlsPullLoop(ctx, s)
		} else {
			go httpPullLoop(ctx, s)
		}
		return nil
	}
	return fmt.Errorf("input nesuportat: %s (foloseste udp://, rtp://, http://, https://)", s.input)
}

func stripRTP(b []byte) []byte {
	if len(b) < 12 || b[0] == 0x47 || b[0]&0xC0 != 0x80 {
		return b
	}
	off := 12 + int(b[0]&0x0F)*4
	if b[0]&0x10 != 0 {
		if len(b) < off+4 {
			return nil
		}
		off += 4 + int(binary.BigEndian.Uint16(b[off+2:]))*4
	}
	if len(b) <= off {
		return nil
	}
	p := b[off:]
	if b[0]&0x20 != 0 {
		pad := int(p[len(p)-1])
		if pad <= len(p) {
			p = p[:len(p)-pad]
		}
	}
	return p
}

var pullClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 8 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ResponseHeaderTimeout: 10 * time.Second,
		MaxIdleConnsPerHost:   4,
	},
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func httpPullLoop(ctx context.Context, s *Source) {
	for ctx.Err() == nil {
		err := httpPullOnce(ctx, s)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.setErr(err.Error())
		}
		if !sleepCtx(ctx, 2*time.Second) {
			return
		}
	}
}

func httpPullOnce(ctx context.Context, s *Source) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", s.input, nil)
	req.Header.Set("User-Agent", "ProxyHub/"+version)
	resp, err := pullClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	s.setErr("")
	// watchdog: abort if no data for 15s
	alive := make(chan struct{}, 1)
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	go func() {
		t := time.NewTimer(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-alive:
				t.Reset(15 * time.Second)
			case <-t.C:
				resp.Body.Close()
				return
			}
		}
	}()
	r := bufio.NewReaderSize(resp.Body, 256*1024)
	for {
		buf := make([]byte, 188*7*4)
		n, err := io.ReadAtLeast(r, buf, 188*7)
		if n > 0 {
			s.publish(buf[:n])
			select {
			case alive <- struct{}{}:
			default:
			}
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return fmt.Errorf("conexiune inchisa de sursa")
			}
			return err
		}
	}
}

// ---------- HLS input (pull .m3u8 and stitch segments into a TS stream) ----------

func fetchText(ctx context.Context, u string) (string, string, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	req.Header.Set("User-Agent", "ProxyHub/"+version)
	resp, err := pullClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return string(b), resp.Request.URL.String(), err
}

func resolveRef(base, ref string) string {
	b, err := url.Parse(base)
	if err != nil {
		return ref
	}
	r, err := url.Parse(strings.TrimSpace(ref))
	if err != nil {
		return ref
	}
	return b.ResolveReference(r).String()
}

func hlsPullLoop(ctx context.Context, s *Source) {
	playlist := s.input
	var lastSeq int64 = -1
	for ctx.Err() == nil {
		text, final, err := fetchText(ctx, playlist)
		if err != nil {
			s.setErr("HLS: " + err.Error())
			if !sleepCtx(ctx, 3*time.Second) {
				return
			}
			continue
		}
		lines := strings.Split(strings.ReplaceAll(text, "\r", ""), "\n")
		// master playlist -> pick highest bandwidth variant
		if strings.Contains(text, "#EXT-X-STREAM-INF") {
			best, bestBW := "", -1
			for i, l := range lines {
				if strings.HasPrefix(l, "#EXT-X-STREAM-INF") {
					bw := 0
					if k := strings.Index(l, "BANDWIDTH="); k >= 0 {
						v := l[k+10:]
						if e := strings.IndexAny(v, ","); e >= 0 {
							v = v[:e]
						}
						bw, _ = strconv.Atoi(v)
					}
					for j := i + 1; j < len(lines); j++ {
						if t := strings.TrimSpace(lines[j]); t != "" && !strings.HasPrefix(t, "#") {
							if bw > bestBW {
								best, bestBW = resolveRef(final, t), bw
							}
							break
						}
					}
				}
			}
			if best == "" {
				s.setErr("HLS: master fara variante")
				sleepCtx(ctx, 5*time.Second)
				continue
			}
			playlist = best
			continue
		}
		var seq int64
		target := 6.0
		var segs []string
		for _, l := range lines {
			switch {
			case strings.HasPrefix(l, "#EXT-X-MEDIA-SEQUENCE:"):
				seq, _ = strconv.ParseInt(strings.TrimSpace(l[22:]), 10, 64)
			case strings.HasPrefix(l, "#EXT-X-TARGETDURATION:"):
				if v, err := strconv.ParseFloat(strings.TrimSpace(l[22:]), 64); err == nil && v > 0 {
					target = v
				}
			case l != "" && !strings.HasPrefix(l, "#"):
				segs = append(segs, resolveRef(final, l))
			}
		}
		if lastSeq < 0 && len(segs) > 3 { // start near live edge
			lastSeq = seq + int64(len(segs)) - 4
		}
		got := false
		for i, su := range segs {
			sn := seq + int64(i)
			if sn <= lastSeq {
				continue
			}
			if err := pullSegment(ctx, s, su); err != nil {
				s.setErr("HLS seg: " + err.Error())
				break
			}
			lastSeq = sn
			got = true
			s.setErr("")
		}
		wait := time.Duration(target/2*1000) * time.Millisecond
		if !got {
			wait = time.Duration(target/3*1000) * time.Millisecond
		}
		if wait < 500*time.Millisecond {
			wait = 500 * time.Millisecond
		}
		if !sleepCtx(ctx, wait) {
			return
		}
	}
}

func pullSegment(ctx context.Context, s *Source, u string) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	req.Header.Set("User-Agent", "ProxyHub/"+version)
	resp, err := pullClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	r := bufio.NewReaderSize(resp.Body, 128*1024)
	for {
		buf := make([]byte, 188*7*4)
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			s.publish(buf[:n])
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
	}
}

func isClosedErr(err error) bool { return errors.Is(err, net.ErrClosed) }
