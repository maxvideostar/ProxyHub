//go:build windows

package main

import (
	"encoding/binary"
	"net"
	"syscall"
	"unsafe"
)

const ipPktInfo = 19 // IP_PKTINFO on Windows

func reuseAddrControl(network, address string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	})
	if err != nil {
		return err
	}
	return serr
}

func enablePktInfo(c *net.UDPConn) bool {
	rc, err := c.SyscallConn()
	if err != nil {
		return false
	}
	var serr error
	rc.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_IP, ipPktInfo, 1)
	})
	return serr == nil
}

// WSACMSGHDR { SIZE_T cmsg_len; INT cmsg_level; INT cmsg_type; } + data
// IN_PKTINFO { IN_ADDR ipi_addr; ULONG ipi_ifindex; }
func parseDst(oob []byte) (d [4]byte, ok bool) {
	ptr := int(unsafe.Sizeof(uintptr(0)))
	align := func(x int) int { return (x + ptr - 1) &^ (ptr - 1) }
	hdr := align(ptr + 8)
	for len(oob) >= hdr {
		var l int
		if ptr == 8 {
			l = int(binary.LittleEndian.Uint64(oob))
		} else {
			l = int(binary.LittleEndian.Uint32(oob))
		}
		level := int32(binary.LittleEndian.Uint32(oob[ptr:]))
		typ := int32(binary.LittleEndian.Uint32(oob[ptr+4:]))
		if l < hdr || l > len(oob) {
			return d, false
		}
		if level == syscall.IPPROTO_IP && typ == ipPktInfo && l >= hdr+4 {
			copy(d[:], oob[hdr:hdr+4])
			return d, true
		}
		next := align(l)
		if next > len(oob) {
			break
		}
		oob = oob[next:]
	}
	return d, false
}

func joinGroup(c *net.UDPConn, group, ifIP [4]byte, join bool) error {
	rc, err := c.SyscallConn()
	if err != nil {
		return err
	}
	opt := syscall.IP_ADD_MEMBERSHIP
	if !join {
		opt = syscall.IP_DROP_MEMBERSHIP
	}
	mreq := &syscall.IPMreq{Multiaddr: group, Interface: ifIP}
	var serr error
	rc.Control(func(fd uintptr) {
		serr = syscall.SetsockoptIPMreq(syscall.Handle(fd), syscall.IPPROTO_IP, opt, mreq)
	})
	return serr
}
