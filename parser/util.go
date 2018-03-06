// Util funcs shared by all parsers
package parser

import (
	"errors"
	"net"
	"strings"
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

// IP validation errors.
var (
	ErrIPIsUnparseable   = errors.New("IP not parsable")
	ErrIPIsUnconvertible = errors.New("IP not convertible to ipv4 or ipv6")
	ErrIPIsZero          = errors.New("IP is zero/unspecified")
	ErrIPIsUnroutable    = errors.New("IP is nonroutable")

	ErrIPv4IsPrivate    = errors.New("private IPv4")
	ErrIPv6IsPrivate    = errors.New("private IPv6")
	ErrIPv4IsUnroutable = errors.New("unroutable IPv4")
	ErrIPv6IsUnroutable = errors.New("unroutable IPv6")

	ErrIPv6MultipleTripleColon = errors.New("more than one ::: in an ip address")
	ErrIPv6QuadColon           = errors.New("IP address contains :::: ")
)

// FixBadIPv6 fixes triple colon ::: which is produced by sidestream.
func FixBadIPv6(ipStr string) (string, error) {
	split := strings.Split(ipStr, ":::")
	switch len(split) {
	case 0:
		fallthrough
	case 1:
		return ipStr, nil
	case 2:
		if split[1][0] == ':' {
			return "", ErrIPv6QuadColon
		}
		return split[0] + "::" + split[1], nil
	default:
		return ipStr, ErrIPv6MultipleTripleColon
	}
}

// ValidateIP validates (and possibly repairs) IP addresses.
// Return nil if it is a valid IPv4 or IPv6 address (or can be repaired), non-nil otherwise.
func ValidateIP(ipStr string) error {
	ipStr, err := FixBadIPv6(ipStr)
	if err != nil {
		return err
	}
	ip := net.ParseIP(ipStr)
	if ip == nil || (ip.To4() == nil && ip.To16() == nil) {
		return ErrIPIsUnparseable
	}
	if ip.To4() == nil && ip.To16() == nil {
		return ErrIPIsUnconvertible
	}
	if ip.IsUnspecified() {
		return ErrIPIsZero
	}

	if ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
		return ErrIPIsUnroutable
	}

	if ip.To4() != nil {
		// Check whether it is a private IP.
		_, private24BitBlock, _ := net.ParseCIDR("10.0.0.0/8")
		_, private20BitBlock, _ := net.ParseCIDR("172.16.0.0/12")
		_, private16BitBlock, _ := net.ParseCIDR("192.168.0.0/16")
		_, private22BitBlock, _ := net.ParseCIDR("100.64.0.0/10")
		if private24BitBlock.Contains(ip) || private20BitBlock.Contains(ip) ||
			private16BitBlock.Contains(ip) || private22BitBlock.Contains(ip) {
			return ErrIPv4IsPrivate
		}

		// check whether it is nonroutable IP
		_, nonroutable1, _ := net.ParseCIDR("0.0.0.0/8")
		_, nonroutable2, _ := net.ParseCIDR("192.0.2.0/24")
		if nonroutable1.Contains(ip) || nonroutable2.Contains(ip) {
			return ErrIPv4IsUnroutable
		}
	} else if ip.To16() != nil {
		_, private7BitBlock, _ := net.ParseCIDR("FC00::/7")
		if private7BitBlock.Contains(ip) {
			return ErrIPv6IsPrivate
		}

		_, nonroutable1, _ := net.ParseCIDR("2001:db8::/32")
		_, nonroutable2, _ := net.ParseCIDR("fec0::/10")
		if nonroutable1.Contains(ip) || nonroutable2.Contains(ip) {
			return ErrIPv6IsUnroutable
		}
	}
	return nil
}
