package netutil

import (
	"net"
	"strings"
)

// IsLoopbackListen reports whether bind is only loopback.
// Empty host, 0.0.0.0, and :: are treated as all-interfaces (not loopback).
func IsLoopbackListen(bind string) bool {
	host := bind
	if h, _, err := net.SplitHostPort(bind); err == nil {
		host = h
	} else if strings.HasPrefix(bind, ":") {
		host = ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// IsPrivateOrLoopbackAddr reports whether addr is loopback or RFC1918/ULA.
func IsPrivateOrLoopbackAddr(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate()
}
