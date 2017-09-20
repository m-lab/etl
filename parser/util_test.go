package parser_test

import (
	"fmt"
	"syscall"
	"testing"

	"github.com/m-lab/etl/parser"
)

func TestParseIPFamily(t *testing.T) {
	if parser.ParseIPFamily("1.2.3.4") != syscall.AF_INET {
		t.Fatalf("IPv4 address not parsed correctly.")
	}
	if parser.ParseIPFamily("2001:db8:0:1:1:1:1:1") != syscall.AF_INET6 {
		t.Fatalf("IPv6 address not parsed correctly.")
	}
}

func TestValidateIP(t *testing.T) {
	if parser.ValidateIP("1.2.3.4") != nil {
		fmt.Println(parser.ValidateIP("1.2.3.4"))
		t.Fatalf("Valid IPv4 was identified as invalid.")
	}
	if parser.ValidateIP("2620:0:1000:2304:8053:fe91:6e2e:b4f1") != nil {
		t.Fatalf("Valid IPv6 was identified as invalid.")
	}
	if parser.ValidateIP("::") == nil || parser.ValidateIP("0.0.0.0") == nil ||
		parser.ValidateIP("abc.0.0.0") == nil || parser.ValidateIP("1.0.0.256") == nil {
		t.Fatalf("Invalid IP was identified as valid.")
	}
	if parser.ValidateIP("172.16.0.1") == nil {
		t.Fatalf("Private IP was not identified as invalid IP.")
	}
	if parser.ValidateIP("127.0.0.1") == nil || parser.ValidateIP("::ffff:127.0.0.1") == nil {
		t.Fatalf("Nonroutable IP was not identified as invalid IP.")
	}
}

func BenchmarkValidateIPv4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = parser.ValidateIP("1.2.3.4")
	}
}

func TestRunningTime(t *testing.T) {
	fmt.Println(testing.Benchmark(BenchmarkValidateIPv4))
}
