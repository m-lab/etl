// Util funcs shared by all parsers
package parser

import (
	"errors"
	"net"
	"syscall"
)

func ParseIPFamily(ipStr string) int64 {
	ip := net.ParseIP(ipStr)
	if ip.To4() != nil {
		return syscall.AF_INET
	} else if ip.To16() != nil {
		return syscall.AF_INET6
	}
	return -1
}

// Return nil if it is a valid IP v4 or IPv6 address.
func ValidateIP(ipStr string) error {
	ip := net.ParseIP(ipStr)
	if ip == nil || ip.To4() == nil && ip.To16() == nil {
		return errors.New("IP not parsable.")
	}
	if ip.Equal(net.IPv4allrouter) || ip.Equal(net.IPv6linklocalallrouters) ||
		ip.Equal(net.IPv6zero) || ip.Equal(net.IPv4zero) {
		return errors.New("zero IP is invalid.")
	}
	// Check whether it is a private IP.
	_, private24BitBlock, _ := net.ParseCIDR("10.0.0.0/8")
	_, private20BitBlock, _ := net.ParseCIDR("172.16.0.0/12")
	_, private16BitBlock, _ := net.ParseCIDR("192.168.0.0/16")
	_, private22BitBlock, _ := net.ParseCIDR("100.64.0.0/10")
	if ip.To4() != nil {
		if private24BitBlock.Contains(ip) || private20BitBlock.Contains(ip) ||
			private16BitBlock.Contains(ip) || private22BitBlock.Contains(ip) {
			return errors.New("private IPv4 is invalid.")
		}
	}

	_, private7BitBlock, _ := net.ParseCIDR("FC00::/7")
	if ip.To16() != nil && private7BitBlock.Contains(ip) {
		return errors.New("private IPv6 is invalid.")
	}

	// check whether it is nonroutable IP
	if ip.To4() != nil {
		_, nonroutable1, _ := net.ParseCIDR("0.0.0.0/8")
		_, nonroutable2, _ := net.ParseCIDR("127.0.0.0/8")
		_, nonroutable3, _ := net.ParseCIDR("192.0.2.0/24")
		_, nonroutable4, _ := net.ParseCIDR("240.0.0.0/4")
		_, nonroutable5, _ := net.ParseCIDR("169.254.0.0/16")
		if nonroutable1.Contains(ip) || nonroutable2.Contains(ip) ||
			nonroutable3.Contains(ip) || nonroutable4.Contains(ip) || nonroutable5.Contains(ip) {
			return errors.New("Nonroutable IPv4 is invalid")
		}
	}

	if ip.To16() != nil {
		_, nonroutable1, _ := net.ParseCIDR("::/128")
		_, nonroutable2, _ := net.ParseCIDR("::1/128")
		_, nonroutable3, _ := net.ParseCIDR("2001:db8::/32")
		_, nonroutable4, _ := net.ParseCIDR("fec0::/10")
		_, nonroutable5, _ := net.ParseCIDR("fe80::/10")
		_, nonroutable6, _ := net.ParseCIDR("ff00::/8")
		if nonroutable1.Contains(ip) || nonroutable2.Contains(ip) ||
			nonroutable3.Contains(ip) || nonroutable4.Contains(ip) ||
			nonroutable5.Contains(ip) || nonroutable6.Contains(ip) {
			return errors.New("Nonroutable IPv6 is invalid")
		}
	}
	return nil
}
