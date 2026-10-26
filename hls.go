package main

import (
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Lightweight HLS segmenter working directly on MPEG-TS (no re-mux, original PIDs kept).
// Segments are cut on video keyframes (random_access_indicator) when available.

type hlsSeg struct {
	seq  uint64
	dur  float64
	data []byte
}

type Segmenter struct {
	name       string
	input      string
	src        *Source
	sub        *Sub
	mu         sync.RWMutex
	segs       []*hlsSeg
	seq        uint64
	cur        []byte
	curStart   time.Time
	started    bool
	lastAccess atomic.Int64
	pat, pmt   []byte
	pmtPID     int
	cutPID     int
	sawRAI     bool
	carry      []byte
	readyOnce  sync.Once
	ready      chan struct{}
	stop       chan struct{}
	target     float64
	window     int
}

type hlsMgr struct {
	mu   sync.Mutex
	segs map[string]*Segmenter
}

var hlsm = &hlsMgr{segs: map[string]*Segmenter{}}

func (m *hlsMgr) Get(ch Channel) (*Segmenter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strings.ToLower(ch.Name)
	if s := m.segs[key]; s != nil && s.input == ch.Input {
		s.lastAccess.Store(time.Now().UnixNano())
		return s, nil
	} else if s != nil {
		s.Close()
		delete(m.segs, key)
	}
	src, err := hub.Acquire(ch.Input)
	if err != nil {
		return nil, err
	}
	c := getConfig()
	s := &Segmenter{name: ch.Name, input: ch.Input, src: src, sub: src.Subscribe(),
		ready: make(chan struct{}), stop: make(chan struct{}), pmtPID: -1, cutPID: -1,
		target: float64(c.HLSSegment), window: c.HLSWindow}
	s.lastAccess.Store(time.Now().UnixNano())
	m.segs[key] = s
	go s.run(func() {
		m.mu.Lock()
		if m.segs[key] == s {
			delete(m.segs, key)
		}
		m.mu.Unlock()
	})
	log.Printf("[hls] start %s", ch.Name)
	return s, nil
}

func (m *hlsMgr) Drop(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strings.ToLower(name)
	if s := m.segs[key]; s != nil {
		s.Close()
		delete(m.segs, key)
	}
}

func (m *hlsMgr) Active(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.segs[strings.ToLower(name)] != nil
}

func (s *Segmenter) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
}

func (s *Segmenter) run(onExit func()) {
	defer func() {
		s.src.Unsubscribe(s.sub)
		hub.Release(s.src)
		onExit()
		log.Printf("[hls] stop %s", s.name)
	}()
	idle := time.NewTicker(5 * time.Second)
	defer idle.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-idle.C:
			if time.Since(time.Unix(0, s.lastAccess.Load())) > 45*time.Second {
				return
			}
		case d := <-s.sub.ch:
			s.feed(d)
		}
	}
}

func (s *Segmenter) feed(d []byte) {
	if len(s.carry) > 0 {
		d = append(s.carry, d...)
		s.carry = nil
	}
	i := 0
	for i+188 <= len(d) {
		if d[i] != 0x47 {
			i++
			continue
		}
		s.packet(d[i : i+188])
		i += 188
	}
	if i < len(d) {
		s.carry = append([]byte(nil), d[i:]...)
	}
}

func (s *Segmenter) packet(p []byte) {
	pid := int(p[1]&0x1F)<<8 | int(p[2])
	pusi := p[1]&0x40 != 0
	afc := (p[3] >> 4) & 3
	off := 4
	rai := false
	if afc&2 != 0 {
		if p[4] > 0 && p[5]&0x40 != 0 {
			rai = true
		}
		off += 1 + int(p[4])
	}
	hasPayload := afc&1 != 0 && off < 188

	if pid == 0 && pusi && hasPayload {
		s.pat = append(s.pat[:0], p...)
		s.parsePAT(p[off:])
	} else if pid == s.pmtPID && pusi && hasPayload {
		s.pmt = append(s.pmt[:0], p...)
		s.parsePMT(p[off:])
	}
	if rai && pid == s.cutPID {
		s.sawRAI = true
	}

	if s.cutPID < 0 || s.pat == nil || s.pmt == nil {
		return // wait for PSI
	}
	isCut := pid == s.cutPID && pusi && (rai || !s.sawRAI)
	if !s.started {
		if !isCut {
			return
		}
		s.started = true
		s.newSeg(p)
		return
	}
	el := time.Since(s.curStart).Seconds()
	if (isCut && el >= s.target) || (pid == s.cutPID && pusi && el >= s.target*3) || el >= s.target*5 {
		s.finalize(el)
		s.newSeg(p)
		return
	}
	s.cur = append(s.cur, p...)
}

func (s *Segmenter) newSeg(first []byte) {
	s.cur = make([]byte, 0, 2<<20)
	s.cur = append(s.cur, s.pat...)
	s.cur = append(s.cur, s.pmt...)
	s.cur = append(s.cur, first...)
	s.curStart = time.Now()
}

func (s *Segmenter) finalize(dur float64) {
	seg := &hlsSeg{seq: s.seq, dur: dur, data: s.cur}
	s.seq++
	s.mu.Lock()
	s.segs = append(s.segs, seg)
	if len(s.segs) > s.window+2 {
		s.segs = s.segs[len(s.segs)-(s.window+2):]
	}
	s.mu.Unlock()
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *Segmenter) parsePAT(b []byte) {
	if len(b) < 1 {
		return
	}
	t := 1 + int(b[0])
	if t+8 > len(b) || b[t] != 0x00 {
		return
	}
	sl := int(b[t+1]&0x0F)<<8 | int(b[t+2])
	end := t + 3 + sl - 4
	if end > len(b) {
		end = len(b)
	}
	for i := t + 8; i+4 <= end; i += 4 {
		prog := int(b[i])<<8 | int(b[i+1])
		if prog != 0 {
			s.pmtPID = int(b[i+2]&0x1F)<<8 | int(b[i+3])
			return
		}
	}
}

func (s *Segmenter) parsePMT(b []byte) {
	if len(b) < 1 {
		return
	}
	t := 1 + int(b[0])
	if t+12 > len(b) || b[t] != 0x02 {
		return
	}
	sl := int(b[t+1]&0x0F)<<8 | int(b[t+2])
	end := t + 3 + sl - 4
	if end > len(b) {
		end = len(b)
	}
	pil := int(b[t+10]&0x0F)<<8 | int(b[t+11])
	video, audio := -1, -1
	for i := t + 12 + pil; i+5 <= end; {
		st := b[i]
		epid := int(b[i+1]&0x1F)<<8 | int(b[i+2])
		eil := int(b[i+3]&0x0F)<<8 | int(b[i+4])
		switch st {
		case 0x01, 0x02, 0x10, 0x1B, 0x24, 0x42, 0xEA:
			if video < 0 {
				video = epid
			}
		case 0x03, 0x04, 0x0F, 0x11, 0x81, 0x87, 0x06:
			if audio < 0 {
				audio = epid
			}
		}
		i += 5 + eil
	}
	if video >= 0 {
		s.cutPID = video
	} else if audio >= 0 {
		s.cutPID = audio
	}
}

func (s *Segmenter) Playlist(token string) (string, bool) {
	select {
	case <-s.ready:
	case <-time.After(15 * time.Second):
		return "", false
	}
	s.lastAccess.Store(time.Now().UnixNano())
	s.mu.RLock()
	defer s.mu.RUnlock()
	segs := s.segs
	if len(segs) > s.window {
		segs = segs[len(segs)-s.window:]
	}
	maxd := s.target
	for _, g := range segs {
		maxd = math.Max(maxd, g.dur)
	}
	q := ""
	if token != "" {
		q = "?token=" + token
	}
	var sb strings.Builder
	sb.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	fmt.Fprintf(&sb, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(maxd)))
	fmt.Fprintf(&sb, "#EXT-X-MEDIA-SEQUENCE:%d\n", segs[0].seq)
	for _, g := range segs {
		fmt.Fprintf(&sb, "#EXTINF:%.3f,\n/hls/%s/%d.ts%s\n", g.dur, s.name, g.seq, q)
	}
	return sb.String(), true
}

func (s *Segmenter) Segment(seq uint64) []byte {
	s.lastAccess.Store(time.Now().UnixNano())
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, g := range s.segs {
		if g.seq == seq {
			return g.data
		}
	}
	return nil
}
