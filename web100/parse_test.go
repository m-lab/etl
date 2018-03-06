package web100_test

import (
	"bytes"
	"fmt"
	"syscall"
	"testing"

	"github.com/m-lab/etl/web100"
)

// shortTcpKisTxt is a snippet from the tcp-kis.txt file. It includes variables with
// two legacy names, a single legacy name, and no legacy name.
const shortTcpKisTxt = `
------------------------------------------------------------------------------
VariableName:	StartTimeStamp
Scope:		both
RenameFrom:	StartTimeSec StartTime
ShortDescr:	Start Time
ProcType:	Integer32
SNMPType:	DateAndTime
Description:	Time at which this row was created and all ZeroBasedCounters in
		the row were initialized to zero.
R/W:		read-only
------------------------------------------------------------------------------
		The following objects can be used to fit minimal
		performance models to the TCP data rate.
------------------------------------------------------------------------------
VariableName:	CurMSS
Scope:		both
RenameFrom:	CurrentMSS
ShortDescr:	Current Maximum Segment Size
Refer:		RFC1122, Requirements for Internet Hosts - Communication Layers
Units:		octets
ProcType:	Gauge32
Description:	The current maximum segment size (MSS), in octets.
R/W:		read-only
------------------------------------------------------------------------------
VariableName:	PipeSize
Scope:		RFC4898
ShortDescr:	Octets in flight
Refer:		RFC793, RFC2581, RFC3517
Units:		octets
ProcType:	Gauge32
Description:	The TCP senders current estimate of the number of
		unacknowledged data octets in the network.

		While not in recovery (e.g., while the receiver is not reporting
		missing data to the sender) this is precisely the same as
`

func TestParseWeb100Definitions(t *testing.T) {
	expectedNames := map[string]string{
		"CurrentMSS":   "CurMSS",
		"StartTime":    "StartTimeStamp",
		"StartTimeSec": "StartTimeStamp",
	}

	r := bytes.NewBufferString(shortTcpKisTxt)
	actualNames, err := web100.ParseWeb100Definitions(r)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(actualNames) != len(expectedNames) {
		t.Errorf("Returned map size does not match: got %d; want %d",
			len(actualNames), len(expectedNames))
	}

	for legacyKey, preferedName := range expectedNames {
		if v, ok := actualNames[legacyKey]; !ok || v != preferedName {
			t.Errorf("Missing legacy variable %q: got %q; want %q",
				legacyKey, v, preferedName)
		}
	}
}

func TestParseIPFamily(t *testing.T) {
	if web100.ParseIPFamily("1.2.3.4") != syscall.AF_INET {
		t.Fatalf("IPv4 address not parsed correctly.")
	}
	if web100.ParseIPFamily("2001:db8:0:1:1:1:1:1") != syscall.AF_INET6 {
		t.Fatalf("IPv6 address not parsed correctly.")
	}
}

func TestValidateIP(t *testing.T) {
	if web100.ValidateIP("1.2.3.4") != nil {
		fmt.Println(web100.ValidateIP("1.2.3.4"))
		t.Fatalf("Valid IPv4 was identified as invalid.")
	}
	if web100.ValidateIP("2620:0:1000:2304:8053:fe91:6e2e:b4f1") != nil {
		t.Fatalf("Valid IPv6 was identified as invalid.")
	}
	if web100.ValidateIP("::") == nil || web100.ValidateIP("0.0.0.0") == nil ||
		web100.ValidateIP("abc.0.0.0") == nil || web100.ValidateIP("1.0.0.256") == nil {
		t.Fatalf("Invalid IP was identified as valid.")
	}
	if web100.ValidateIP("172.16.0.1") == nil {
		t.Fatalf("Private IP was not identified as invalid IP.")
	}
	if web100.ValidateIP("127.0.0.1") == nil || web100.ValidateIP("::ffff:127.0.0.1") == nil {
		t.Fatalf("Nonroutable IP was not identified as invalid IP.")
	}

	if web100.ValidateIP("2001:668:1f:22:::81") != nil {
		t.Fatalf("IPv6 with triple colon was not repaired.")
	}
	if web100.ValidateIP("2001:668:1f::::81") == nil {
		t.Fatalf("IPv6 with quad colon was allowed.")
	}
}

// To run benchmark...
// go test -bench=. ./parser/...
func BenchmarkValidateIPv4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = web100.ValidateIP("1.2.3.4")
	}
}
