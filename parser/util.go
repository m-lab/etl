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
	if ip == nil || (ip.To4() == nil && ip.To16() == nil) {
		return errors.New("IP not parsable.")
	}
	if ip.IsUnspecified() {
		return errors.New("IP is zero and invalid.")
	}

	if ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
		return errors.New("Nonroutable IP is invalid")
	}
	
	if ip.To4() != nil {
		// Check whether it is a private IP.
		_, private24BitBlock, _ := net.ParseCIDR("10.0.0.0/8")
		_, private20BitBlock, _ := net.ParseCIDR("172.16.0.0/12")
		_, private16BitBlock, _ := net.ParseCIDR("192.168.0.0/16")
		_, private22BitBlock, _ := net.ParseCIDR("100.64.0.0/10")
		if private24BitBlock.Contains(ip) || private20BitBlock.Contains(ip) ||
			private16BitBlock.Contains(ip) || private22BitBlock.Contains(ip) {
			return errors.New("Private IPv4 is invalid.")
		}

		// check whether it is nonroutable IP
		_, nonroutable1, _ := net.ParseCIDR("0.0.0.0/8")
		_, nonroutable2, _ := net.ParseCIDR("192.0.2.0/24")
		if nonroutable1.Contains(ip) || nonroutable2.Contains(ip) {
			return errors.New("Nonroutable IPv4 is invalid")
		}
	} else if ip.To16() != nil {
		_, private7BitBlock, _ := net.ParseCIDR("FC00::/7")
		if private7BitBlock.Contains(ip) {
			return errors.New("Private IPv6 is invalid.")
		}

		_, nonroutable1, _ := net.ParseCIDR("2001:db8::/32")
		_, nonroutable2, _ := net.ParseCIDR("fec0::/10")
		if nonroutable1.Contains(ip) || nonroutable2.Contains(ip) {
			return errors.New("Nonroutable IPv6 is invalid")
		}
	}
	return nil
}
