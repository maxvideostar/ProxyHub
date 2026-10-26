//go:build linux

package main

import (
	"net"
	"syscall"
)

func reuseAddrControl(network, address string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
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
		serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_PKTINFO, 1)
	})
	return serr == nil
}

func parseDst(oob []byte) (d [4]byte, ok bool) {
	if len(oob) == 0 {
		return d, false
	}
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return d, false
	}
	for _, m := range msgs {
		if m.Header.Level == syscall.IPPROTO_IP && m.Header.Type == syscall.IP_PKTINFO && len(m.Data) >= 12 {
			copy(d[:], m.Data[8:12]) // in_pktinfo.ipi_addr = header destination
			return d, true
		}
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
		serr = syscall.SetsockoptIPMreq(int(fd), syscall.IPPROTO_IP, opt, mreq)
	})
	return serr
}
