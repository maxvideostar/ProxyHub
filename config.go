package main

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type Channel struct {
	Name     string `json:"name"`
	Title    string `json:"title,omitempty"`
	Input    string `json:"input"`
	Enabled  bool   `json:"enabled"`
	OnDemand bool   `json:"on_demand"`
}

type Config struct {
	Listen       string    `json:"listen"`
	UdpxyListen  string    `json:"udpxy_listen"`
	Interface    string    `json:"interface"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	UdpxyEnabled bool      `json:"udpxy_enabled"`
	StreamToken  string    `json:"stream_token"`
	AllowedIPs   []string  `json:"allowed_ips"`
	HLSSegment   int       `json:"hls_segment"`
	HLSWindow    int       `json:"hls_window"`
	PublicURL    string    `json:"public_url"`
	Channels     []Channel `json:"channels"`
}

var (
	cfgMu   sync.RWMutex
	cfg     *Config
	cfgPath string
)

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_.\-]{1,64}$`)

var reservedNames = map[string]bool{
	"udp": true, "rtp": true, "api": true, "login": true, "logout": true,
	"panel": true, "hls": true, "playlist.m3u": true, "favicon.ico": true, "status": true,
}

func defaultConfig() *Config {
	return &Config{
		Listen:       ":4000",
		UdpxyListen:  ":7000",
		Username:     "admin",
		PasswordHash: hashPassword("admin"),
		UdpxyEnabled: true,
		HLSSegment:   4,
		HLSWindow:    6,
		Channels:     []Channel{},
	}
}

func exeDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	if r, err := filepath.EvalSymlinks(exe); err == nil {
		exe = r
	}
	return filepath.Dir(exe)
}

func loadConfig(path string) error {
	cfgPath = path
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		cfg = defaultConfig()
		return saveConfigLocked()
	}
	if err != nil {
		return err
	}
	c := defaultConfig()
	if err := json.Unmarshal(b, c); err != nil {
		return err
	}
	if c.HLSSegment < 1 {
		c.HLSSegment = 4
	}
	if c.HLSWindow < 3 {
		c.HLSWindow = 6
	}
	if c.Channels == nil {
		c.Channels = []Channel{}
	}
	cfg = c
	return nil
}

// saveConfigLocked writes cfg; caller must hold cfgMu (or be at startup).
func saveConfigLocked() error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := cfgPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, cfgPath)
}

func getConfig() Config {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	c := *cfg
	c.Channels = append([]Channel(nil), cfg.Channels...)
	c.AllowedIPs = append([]string(nil), cfg.AllowedIPs...)
	return c
}

func findChannel(name string) (Channel, bool) {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	for _, ch := range cfg.Channels {
		if strings.EqualFold(ch.Name, name) {
			return ch, true
		}
	}
	return Channel{}, false
}

// ---- passwords (PBKDF2-SHA256) ----

func hashPassword(pw string) string {
	salt := make([]byte, 16)
	rand.Read(salt)
	key, _ := pbkdf2.Key(sha256.New, pw, salt, 120000, 32)
	return "pbkdf2$120000$" + hex.EncodeToString(salt) + "$" + base64.StdEncoding.EncodeToString(key)
}

func checkPassword(hash, pw string) bool {
	p := strings.Split(hash, "$")
	if len(p) != 4 || p[0] != "pbkdf2" {
		return false
	}
	salt, err := hex.DecodeString(p[2])
	if err != nil {
		return false
	}
	want, err := base64.StdEncoding.DecodeString(p[3])
	if err != nil {
		return false
	}
	iter := 0
	for _, c := range p[1] {
		iter = iter*10 + int(c-'0')
	}
	got, err := pbkdf2.Key(sha256.New, pw, salt, iter, len(want))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

func randToken(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
