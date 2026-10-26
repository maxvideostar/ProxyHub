package main

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const sessCookie = "proxyhub_session"

type session struct {
	user    string
	expires time.Time
}

var (
	sessMu   sync.Mutex
	sessions = map[string]*session{}
	failMu   sync.Mutex
	fails    = map[string]*failInfo{}
)

type failInfo struct {
	n     int
	until time.Time
}

func newSession(user string) string {
	tok := randToken(32)
	sessMu.Lock()
	sessions[tok] = &session{user: user, expires: time.Now().Add(7 * 24 * time.Hour)}
	sessMu.Unlock()
	return tok
}

func validSession(r *http.Request) bool {
	c, err := r.Cookie(sessCookie)
	if err != nil {
		return false
	}
	sessMu.Lock()
	defer sessMu.Unlock()
	s := sessions[c.Value]
	if s == nil {
		return false
	}
	if time.Now().After(s.expires) {
		delete(sessions, c.Value)
		return false
	}
	return true
}

func dropSession(r *http.Request) {
	if c, err := r.Cookie(sessCookie); err == nil {
		sessMu.Lock()
		delete(sessions, c.Value)
		sessMu.Unlock()
	}
}

func dropAllSessions() {
	sessMu.Lock()
	sessions = map[string]*session{}
	sessMu.Unlock()
}

func loginLocked(ip string) bool {
	failMu.Lock()
	defer failMu.Unlock()
	f := fails[ip]
	return f != nil && time.Now().Before(f.until)
}

func loginFailed(ip string) {
	failMu.Lock()
	defer failMu.Unlock()
	f := fails[ip]
	if f == nil {
		f = &failInfo{}
		fails[ip] = f
	}
	f.n++
	if f.n >= 5 {
		f.until = time.Now().Add(5 * time.Minute)
		f.n = 0
	}
}

func loginOK(ip string) {
	failMu.Lock()
	delete(fails, ip)
	failMu.Unlock()
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
		if xr := r.Header.Get("X-Real-IP"); xr != "" {
			return xr
		}
	}
	return host
}

// streamAllowed checks IP allow-list and optional stream token for playback URLs.
func streamAllowed(r *http.Request) bool {
	c := getConfig()
	if len(c.AllowedIPs) > 0 {
		ip := net.ParseIP(clientIP(r))
		ok := false
		for _, a := range c.AllowedIPs {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			if strings.Contains(a, "/") {
				if _, n, err := net.ParseCIDR(a); err == nil && ip != nil && n.Contains(ip) {
					ok = true
					break
				}
			} else if pa := net.ParseIP(a); pa != nil && ip != nil && pa.Equal(ip) {
				ok = true
				break
			}
		}
		if !ok && !validSession(r) {
			return false
		}
	}
	if c.StreamToken != "" && r.URL.Query().Get("token") != c.StreamToken && !validSession(r) {
		return false
	}
	return true
}
