package web100

import (
	"errors"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"syscall"
)

// ParseWeb100Definitions reads all web100 variable definitions from tcpKis and
// returns a mapping from legacy names to canonical names. This mapping is
// necessary for translating variable names in archived web100 snapshots to
// canonical variable names.
func ParseWeb100Definitions(tcpKis io.Reader) (map[string]string, error) {
	var legacyNamesToNewNames map[string]string
	var preferredName string

	data, err := ioutil.ReadAll(tcpKis)
	if err != nil {
		return nil, err
	}

	legacyNamesToNewNames = make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		if fields[0] == "VariableName:" {
			preferredName = fields[1]
		}

		if fields[0] == "RenameFrom:" {
			for _, legacyName := range fields[1:] {
				legacyNamesToNewNames[legacyName] = preferredName
			}
		}
	}
	return legacyNamesToNewNames, nil
}

// ParseIPFamily determines whether an IP string is v4 or v6
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

// NormalizeIPv6 fixes triple colon ::: which is produced by sidestream.
// This error is produced by older versions of the c-web100 library, which is still
// used by sidestream.
func NormalizeIPv6(ipStr string) (string, error) {
	split := strings.Split(ipStr, ":::")
	switch len(split) {
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

// NormalizeIP accepts an IPv4 or IPv6 address and returns a normalized version
// of that string. This should be used to fix malformed IPv6 addresses in
// web100 datasets.
func NormalizeIP(ip string) string {
	r, err := NormalizeIPv6(ip)
	if err != nil {
		return ip
	}
	return r
}

// ValidateIP validates (and possibly repairs) IP addresses.
// Return nil if it is a valid IPv4 or IPv6 address (or can be repaired), non-nil otherwise.
func ValidateIP(ipStr string) error {
	ipStr, err := NormalizeIPv6(ipStr)
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
