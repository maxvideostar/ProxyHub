//go:build !linux && !windows

package main

import (
	"net"
	"syscall"
)

func reuseAddrControl(network, address string, c syscall.RawConn) error {
	var serr error
	c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	})
	return serr
}

func enablePktInfo(c *net.UDPConn) bool        { return false }
func parseDst(oob []byte) (d [4]byte, ok bool) { return d, false }

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
