package web100_test

import (
	"bytes"
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
