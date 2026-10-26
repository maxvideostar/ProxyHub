package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
)

// Multicast manager: one socket per (interface, port), bound to 0.0.0.0:port.
// Groups are joined on that socket and packets are demultiplexed by their
// destination address (IP_PKTINFO). This works the same on Windows (where a
// socket cannot be bound to a multicast address) and on Linux, and lets many
// groups that share the same port (very common in IPTV) live side by side.

type mHandler func([]byte)

type mGroup struct {
	handlers map[int]mHandler
	joined   bool
}

type mSock struct {
	key     string
	conn    *net.UDPConn
	ifIP    [4]byte
	mu      sync.RWMutex
	groups  map[[4]byte]*mGroup // zero key = unicast / catch-all
	pktinfo bool
	warned  bool
}

type mcastMgr struct {
	mu     sync.Mutex
	socks  map[string]*mSock
	nextID int
}

var mcast = &mcastMgr{socks: map[string]*mSock{}}

func resolveIfaceIP(name string) ([4]byte, error) {
	var z [4]byte
	if name == "" {
		return z, nil
	}
	if ip := net.ParseIP(name); ip != nil && ip.To4() != nil {
		copy(z[:], ip.To4())
		return z, nil
	}
	ifc, err := net.InterfaceByName(name)
	if err != nil {
		return z, fmt.Errorf("interfata %q: %v", name, err)
	}
	addrs, _ := ifc.Addrs()
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
			copy(z[:], ipn.IP.To4())
			return z, nil
		}
	}
	return z, fmt.Errorf("interfata %q nu are IPv4", name)
}

func (m *mcastMgr) Register(ip net.IP, port int, ifname string, h mHandler) (func(), error) {
	ifIP, err := resolveIfaceIP(ifname)
	if err != nil {
		return nil, err
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil, fmt.Errorf("doar IPv4 este suportat")
	}
	var gkey [4]byte
	multicast := ip4.IsMulticast()
	if multicast {
		copy(gkey[:], ip4)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	skey := net.IP(ifIP[:]).String() + "|" + strconv.Itoa(port)
	s := m.socks[skey]
	if s == nil {
		lc := net.ListenConfig{Control: reuseAddrControl}
		pc, err := lc.ListenPacket(context.Background(), "udp4", "0.0.0.0:"+strconv.Itoa(port))
		if err != nil {
			return nil, fmt.Errorf("bind :%d: %v", port, err)
		}
		conn := pc.(*net.UDPConn)
		conn.SetReadBuffer(8 << 20)
		s = &mSock{key: skey, conn: conn, ifIP: ifIP, groups: map[[4]byte]*mGroup{}}
		s.pktinfo = enablePktInfo(conn)
		if !s.pktinfo {
			log.Printf("[mcast] avertisment: IP_PKTINFO indisponibil pe :%d", port)
		}
		m.socks[skey] = s
		go s.readLoop()
	}

	s.mu.Lock()
	g := s.groups[gkey]
	if g == nil {
		g = &mGroup{handlers: map[int]mHandler{}}
		if multicast {
			if err := joinGroup(conn4(s), gkey, ifIP, true); err != nil {
				s.mu.Unlock()
				if len(s.groups) == 0 {
					s.conn.Close()
					delete(m.socks, skey)
				}
				return nil, fmt.Errorf("join %s: %v", ip4, err)
			}
			g.joined = true
			log.Printf("[mcast] join %s:%d (if %s)", ip4, port, net.IP(ifIP[:]))
		}
		s.groups[gkey] = g
	}
	m.nextID++
	id := m.nextID
	g.handlers[id] = h
	s.mu.Unlock()

	return func() { m.unregister(s, gkey, id) }, nil
}

func conn4(s *mSock) *net.UDPConn { return s.conn }

func (m *mcastMgr) unregister(s *mSock, gkey [4]byte, id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.mu.Lock()
	g := s.groups[gkey]
	if g != nil {
		delete(g.handlers, id)
		if len(g.handlers) == 0 {
			if g.joined {
				joinGroup(s.conn, gkey, s.ifIP, false)
				log.Printf("[mcast] leave %s (%s)", net.IP(gkey[:]), s.key)
			}
			delete(s.groups, gkey)
		}
	}
	empty := len(s.groups) == 0
	s.mu.Unlock()
	if empty {
		s.conn.Close()
		delete(m.socks, s.key)
	}
}

func (s *mSock) readLoop() {
	buf := make([]byte, 65536)
	oob := make([]byte, 512)
	for {
		n, oobn, _, _, err := s.conn.ReadMsgUDP(buf, oob)
		if err != nil {
			if isClosedErr(err) {
				return
			}
			continue
		}
		dst, ok := parseDst(oob[:oobn])
		s.dispatch(dst, ok, buf[:n])
	}
}

func (s *mSock) dispatch(dst [4]byte, ok bool, data []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ok {
		if dst[0] >= 224 && dst[0] <= 239 {
			if g := s.groups[dst]; g != nil {
				for _, h := range g.handlers {
					h(data)
				}
			}
			return
		}
		if g := s.groups[[4]byte{}]; g != nil {
			for _, h := range g.handlers {
				h(data)
			}
		}
		return
	}
	// No destination info: only safe when one group uses this port.
	if len(s.groups) > 1 && !s.warned {
		s.warned = true
		log.Printf("[mcast] %s: mai multe grupuri pe acelasi port fara IP_PKTINFO - fluxurile se pot amesteca", s.key)
	}
	for _, g := range s.groups {
		for _, h := range g.handlers {
			h(data)
		}
	}
}
