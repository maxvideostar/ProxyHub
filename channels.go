package main

import (
	"log"
	"strings"
	"sync"
)

// Always-on channels keep a reference on their source so it never stops.

type heldSrc struct {
	input string
	src   *Source
}

var (
	heldMu sync.Mutex
	held   = map[string]*heldSrc{}
	acqErr = map[string]string{}
)

func applyChannels() {
	c := getConfig()
	want := map[string]Channel{}
	for _, ch := range c.Channels {
		if ch.Enabled && !ch.OnDemand {
			want[strings.ToLower(ch.Name)] = ch
		}
	}
	heldMu.Lock()
	defer heldMu.Unlock()
	for k := range acqErr {
		if _, ok := want[k]; !ok {
			delete(acqErr, k)
		}
	}
	for k, h := range held {
		ch, ok := want[k]
		if !ok || ch.Input != h.input {
			hub.Release(h.src)
			delete(held, k)
		}
	}
	for k, ch := range want {
		if held[k] != nil {
			continue
		}
		src, err := hub.Acquire(ch.Input)
		if err != nil {
			log.Printf("[canal %s] %v", ch.Name, err)
			acqErr[k] = err.Error()
			continue
		}
		delete(acqErr, k)
		held[k] = &heldSrc{input: ch.Input, src: src}
	}
	// drop HLS segmenters for channels removed/disabled/changed
	known := map[string]Channel{}
	for _, ch := range c.Channels {
		known[strings.ToLower(ch.Name)] = ch
	}
	hlsm.mu.Lock()
	var drop []string
	for k, s := range hlsm.segs {
		ch, ok := known[k]
		if !ok || !ch.Enabled || ch.Input != s.input {
			drop = append(drop, k)
		}
	}
	hlsm.mu.Unlock()
	for _, k := range drop {
		hlsm.Drop(k)
	}
}

func channelErr(name string) string {
	heldMu.Lock()
	defer heldMu.Unlock()
	return acqErr[strings.ToLower(name)]
}
