package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	ID       uint64    `json:"id"`
	IP       string    `json:"ip"`
	Kind     string    `json:"kind"` // udpxy | http-ts | hls
	Target   string    `json:"target"`
	UA       string    `json:"ua"`
	Start    time.Time `json:"start"`
	bytes    atomic.Uint64
	rate     atomic.Uint64
	prev     uint64
	lastSeen atomic.Int64
	dropped  func() uint64
	cancel   func()
}

type ClientView struct {
	ID      uint64 `json:"id"`
	IP      string `json:"ip"`
	Kind    string `json:"kind"`
	Target  string `json:"target"`
	UA      string `json:"ua"`
	Seconds int64  `json:"seconds"`
	Bytes   uint64 `json:"bytes"`
	Rate    uint64 `json:"rate"`
	Dropped uint64 `json:"dropped"`
}

type clientReg struct {
	mu     sync.Mutex
	m      map[uint64]*Client
	hls    map[string]*Client
	nextID uint64
}

var clients = &clientReg{m: map[uint64]*Client{}, hls: map[string]*Client{}}

func (r *clientReg) Add(ip, kind, target, ua string, cancel func()) *Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	c := &Client{ID: r.nextID, IP: ip, Kind: kind, Target: target, UA: ua, Start: time.Now(), cancel: cancel}
	c.lastSeen.Store(time.Now().UnixNano())
	r.m[c.ID] = c
	return c
}

func (r *clientReg) Remove(c *Client) {
	r.mu.Lock()
	delete(r.m, c.ID)
	r.mu.Unlock()
}

// HLS clients are stateless HTTP requests; group them by ip+channel.
func (r *clientReg) TouchHLS(ip, target, ua string, n int) *Client {
	key := ip + "|" + target
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.hls[key]
	if c == nil {
		r.nextID++
		c = &Client{ID: r.nextID, IP: ip, Kind: "hls", Target: target, UA: ua, Start: time.Now()}
		r.hls[key] = c
		r.m[c.ID] = c
	}
	c.bytes.Add(uint64(n))
	c.lastSeen.Store(time.Now().UnixNano())
	return c
}

func (r *clientReg) Kick(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.m[id]
	if c == nil {
		return false
	}
	if c.cancel != nil {
		c.cancel()
	} else if c.Kind == "hls" {
		delete(r.hls, c.IP+"|"+c.Target)
		delete(r.m, id)
	}
	return true
}

func (r *clientReg) loop() {
	t := time.NewTicker(2 * time.Second)
	for range t.C {
		now := time.Now().UnixNano()
		r.mu.Lock()
		for k, c := range r.hls {
			if now-c.lastSeen.Load() > int64(30*time.Second) {
				delete(r.hls, k)
				delete(r.m, c.ID)
			}
		}
		for _, c := range r.m {
			b := c.bytes.Load()
			c.rate.Store((b - c.prev) * 8 / 2)
			c.prev = b
		}
		r.mu.Unlock()
	}
}

func (r *clientReg) List() []ClientView {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ClientView, 0, len(r.m))
	for _, c := range r.m {
		v := ClientView{ID: c.ID, IP: c.IP, Kind: c.Kind, Target: c.Target, UA: c.UA,
			Seconds: int64(time.Since(c.Start).Seconds()), Bytes: c.bytes.Load(), Rate: c.rate.Load()}
		if c.dropped != nil {
			v.Dropped = c.dropped()
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *clientReg) CountFor(kind, target string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.m {
		if c.Target == target && (kind == "" || c.Kind == kind) {
			n++
		}
	}
	return n
}
