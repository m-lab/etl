package parser_test

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/go-test/deep"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/rtx"
)

const (
	pcapFilename = "ndt-4c6fb_1625899199_000000000121C1A0.pcap.gz"
	pcapGCSPath  = "gs://archive-measurement-lab/ndt/pcap/2021/07/22/"
)

func TestPCAPParser_ParseAndInsert(t *testing.T) {
	ins := newInMemorySink()
	n := parser.NewPCAPParser(ins, "test", "_suffix", &fakeAnnotator{})

	data, err := ioutil.ReadFile(path.Join("testdata/PCAP/", pcapFilename))
	rtx.Must(err, "failed to load test file")

	date := civil.Date{Year: 2021, Month: 07, Day: 22}

	meta := map[string]bigquery.Value{
		"filename": path.Join(pcapGCSPath, pcapFilename),
		"date":     date,
	}

	if err := n.ParseAndInsert(meta, pcapFilename, data); err != nil {
		t.Errorf("PCAPParser.ParseAndInsert() error = %v, wantErr %v", err, true)
	}

	if n.Accepted() != 1 {
		t.Fatal("Failed to insert snaplog data", ins)
	}
	n.Flush()

	row := ins.data[0].(*schema.PCAPRow)

	expectedParseInfo := schema.ParseInfo{
		Version:    "https://github.com/m-lab/etl/tree/foobar",
		Time:       row.Parser.Time,
		ArchiveURL: path.Join(pcapGCSPath, pcapFilename),
		Filename:   pcapFilename,
		Priority:   0,
		GitCommit:  "12345678",
	}

	expectedPCAPRow := schema.PCAPRow{
		ID:     "ndt-4c6fb_1625899199_000000000121C1A0",
		Parser: expectedParseInfo,
		Date:   date,
	}

	if diff := deep.Equal(row, &expectedPCAPRow); diff != nil {
		t.Errorf("PCAPParser.ParseAndInsert() different row: %s", strings.Join(diff, "\n"))
	}

}

func TestPCAPParser_IsParsable(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		want     bool
	}{
		{
			name:     "success-pcap",
			testName: pcapFilename,
			want:     true,
		},
		{
			name:     "error-bad-extension",
			testName: "badfile.badextension",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := ioutil.ReadFile(path.Join(`testdata/PCAP/`, tt.testName))
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.PCAPParser{}
			_, got := p.IsParsable(tt.testName, data)
			if got != tt.want {
				t.Errorf("PCAPParser.IsParsable() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPCAPParser_GetUUID(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     string
	}{
		{
			name:     "filename-expected-format",
			filename: "2021/07/22/ndt-4c6fb_1625899199_00000000013A4623.pcap.gz",
			want:     "ndt-4c6fb_1625899199_00000000013A4623",
		},
		{
			name:     "filename-without-date-prefix",
			filename: "ndt-4c6fb_1625899199_00000000013A4623.pcap.gz",
			want:     "ndt-4c6fb_1625899199_00000000013A4623",
		},
		{
			name:     "empty-string",
			filename: "",
			want:     ".",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser.PCAPParser{}
			got := p.GetUUID(tt.filename)
			if got != tt.want {
				t.Errorf("PCAPParser.GetUUID() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIPLayer(t *testing.T) {
	type test struct {
		name         string
		fn           string
		packets      int64
		duration     time.Duration
		srcIP, dstIP string
		TTL          uint8
		length       uint16
	}
	tests := []test{
		{name: "retransmits", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz",
			packets: 336, duration: 15409174000, length: 40, srcIP: "173.49.19.128"},
		{name: "ipv6", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA8.pcap.gz",
			packets: 15, duration: 134434000, length: 40, srcIP: "2a0d:5600:24:a71::1d"},
		{name: "protocolErrors2", fn: "testfiles/ndt-nnwk2_1611335823_00000000000C2DA9.pcap.gz",
			packets: 5180, duration: 13444117000, length: 40, srcIP: "2a0d:5600:24:a71::1d"},
	}
	for _, tt := range tests {
		f, err := os.Open(tt.fn)
		if err != nil {
			t.Fatal(err)
		}
		data, err := ioutil.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		packets, err := parser.GetPackets(data)
		if err != nil {
			t.Fatal(err)
		}
		start := packets[0].Ci.Timestamp
		end := packets[len(packets)-1].Ci.Timestamp
		duration := end.Sub(start)
		if duration != tt.duration {
			t.Errorf("%s: duration = %v, want %v", tt.name, duration, tt.duration)
		}
		if len(packets) != int(tt.packets) {
			t.Errorf("%s: expected %d packets, got %d", tt.name, tt.packets, len(packets))
		}
		srcIP, _, _, length, err := packets[0].FastExtractIPFields()
		if err != nil {
			t.Fatal(err)
		}
		if srcIP.String() != tt.srcIP {
			t.Errorf("%s: expected srcIP %s, got %s", tt.name, tt.srcIP, srcIP.String())
		}
		if length != tt.length {
			t.Errorf("%s: expected length %d, got %d", tt.name, tt.length, length)
		}
	}
}

// BenchmarkGetPackets-8   	    3757	    334434 ns/op	  165144 B/op	     381 allocs/op
// RunParallel:
// GetPacket no append:  BenchmarkGetPackets-8   	   15490	     84929 ns/op	   85784 B/op	     375 allocs/op
// Just packet decoding: BenchmarkGetPackets-8   	    8678	    128426 ns/op	  165146 B/op	     381 allocs/op
// Custom Eth and IP:    BenchmarkGetPackets-8   	    7783	    146998 ns/op	  182428 B/op	     381 allocs/op
// With IP decoding:     BenchmarkGetPackets-8   	    4279	    285547 ns/op	  376125 B/op	    1729 allocs/op
// Extract tcp lengths:  BenchmarkGetPackets-8   	    3822	    295116 ns/op	  392779 B/op	    1725 allocs/op
// Custom lengths:       BenchmarkGetPackets-8   	    8372	    159532 ns/op	  182427 B/op	     381 allocs/op
func BenchmarkGetPackets(b *testing.B) {
	f, err := os.Open("testfiles/ndt-nnwk2_1611335823_00000000000C2DFE.pcap.gz")
	if err != nil {
		b.Fatal(err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pkts, err := parser.GetPackets(data)
			if err != nil {
				b.Fatal(err)
			}
			total := 0
			for i := range pkts {
				if false {
					_, _, _, l, err := pkts[i].SlowGetIP()
					if err != nil {
						b.Fatal(err)
					}
					total += int(l)
				} else {
					if err := pkts[i].GetLayers(); err != nil {
						b.Fatal(err)
					}
					total += pkts[i].TCPLength()
				}
			}
			if total != 167003 {
				b.Errorf("total = %d, want %d", total, len(data))
			}
		}
	})

}
