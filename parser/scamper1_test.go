package parser_test

import (
	"errors"
	"io/ioutil"
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
	parserTRC "github.com/m-lab/traceroute-caller/parser"
	"github.com/m-lab/traceroute-caller/tracer"
)

func TestScamper1Parser_ParseAndInsert(t *testing.T) {
	ins := newInMemorySink()
	n := parser.NewScamper1Parser(ins, "test", "_suffix", &fakeAnnotator{})

	file := "valid.jsonl"
	data, err := ioutil.ReadFile(path.Join("testdata/Scamper1/", file))
	rtx.Must(err, "failed to load test file")

	meta := map[string]bigquery.Value{
		"filename": file,
		"date":     civil.Date{Year: 2021, Month: 9, Day: 14},
	}

	err = n.ParseAndInsert(meta, file, data)
	if err != nil {
		t.Errorf("failed to parse scamper1 file: %v", err)
	}

	if n.Accepted() != 1 {
		t.Error("failed to insert snaplog data")
	}
	n.Flush()

	row := ins.data[0].(*schema.Scamper1Row)

	expectedRow := expectedScamper1Row()
	expectedRow.Parser.Time = row.Parser.Time
	if diff := deep.Equal(row, &expectedRow); diff != nil {
		t.Errorf("failed to extract correct row from file: different rows - %s", strings.Join(diff, "\n"))
	}
}

func TestScamper1Parser_ParserAndInsertError(t *testing.T) {
	tests := []struct {
		name    string
		date    civil.Date
		wantErr parser.ErrIsInvalid
	}{
		{
			name: "legacy-date",
			date: civil.Date{
				Year:  2021,
				Month: time.September,
				Day:   8,
			},
			wantErr: parser.ErrIsInvalid{
				File:     "badformat.jsonl",
				Err:      errors.New("invalid traceroute file"),
				IsLegacy: true,
			},
		},
		{
			name: "scamper1-date",
			date: civil.Date{
				Year:  2021,
				Month: time.September,
				Day:   10,
			},
			wantErr: parser.ErrIsInvalid{
				File:     "badformat.jsonl",
				Err:      errors.New("invalid traceroute file"),
				IsLegacy: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := newInMemorySink()
			n := parser.NewScamper1Parser(ins, "test", "_suffix", &fakeAnnotator{})

			file := "badformat.jsonl"
			data, err := ioutil.ReadFile(path.Join("testdata/Scamper1/", file))
			rtx.Must(err, "failed to load test file")

			meta := map[string]bigquery.Value{
				"filename": file,
				"date":     tt.date,
			}

			err = n.ParseAndInsert(meta, file, data)
			invalidErr := parser.ErrIsInvalid{}

			if !errors.As(err, &invalidErr) || invalidErr.IsLegacy != tt.wantErr.IsLegacy {
				t.Errorf("Scamper1Parser.ParseAndInsert() = %v, want = %v", err, tt.wantErr)
			}
		})
	}
}

func TestScamper1_IsParsable(t *testing.T) {
	tests := []struct {
		file string
		want bool
	}{
		{
			file: "valid.jsonl",
			want: true,
		},
		{
			file: "badfile.badextension",
			want: false,
		},
		{
			file: "simple.json",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			data, err := ioutil.ReadFile(path.Join(`testdata/Scamper1/`, tt.file))
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.Scamper1Parser{}
			_, got := p.IsParsable(tt.file, data)
			if got != tt.want {
				t.Errorf("failed to detect if file is parsable: got = %v, want %v", got, tt.want)
			}
		})
	}
}

func expectedScamper1Row() schema.Scamper1Row {
	parseInfo := schema.ParseInfo{
		Version:    "https://github.com/m-lab/etl/tree/foobar",
		Time:       time.Time{},
		ArchiveURL: "valid.jsonl",
		Filename:   "valid.jsonl",
		Priority:   0,
		GitCommit:  "12345678",
	}

	date := civil.Date{Year: 2021, Month: 9, Day: 14}

	bqScamperLinkArray1 := make([]schema.BQScamperLinkArray, 1)
	bqScamperLinkArray1[0] = schema.BQScamperLinkArray{
		Links: []parserTRC.ScamperLink{{
			Addr: "2001:550:3::1ca",
			Probes: []parserTRC.Probe{
				{
					Tx:      parserTRC.TS{Sec: 1567900908, Usec: 979595},
					Replyc:  1,
					TTL:     2,
					Attempt: 0,
					Flowid:  1,
					Replies: []parserTRC.Reply{{
						Rx:       parserTRC.TS{Sec: 1567900909, Usec: 16398},
						TTL:      63,
						RTT:      36.803,
						IcmpType: 3,
						IcmpCode: 0,
						IcmpQTos: 0,
						IcmpQTTL: 1,
					}},
				},
				{
					Tx:      parserTRC.TS{Sec: 1567900909, Usec: 229642},
					Replyc:  1,
					TTL:     2,
					Attempt: 0,
					Flowid:  2,
					Replies: []parserTRC.Reply{{
						Rx:       parserTRC.TS{Sec: 1567900909, Usec: 229974},
						TTL:      63,
						RTT:      0.332,
						IcmpType: 3,
						IcmpCode: 0,
						IcmpQTos: 0,
						IcmpQTTL: 1,
					}},
				},
				{
					Tx:      parserTRC.TS{Sec: 1567900909, Usec: 480242},
					Replyc:  1,
					TTL:     2,
					Attempt: 0,
					Flowid:  3,
					Replies: []parserTRC.Reply{{
						Rx:       parserTRC.TS{Sec: 1567900909, Usec: 480571},
						TTL:      63,
						RTT:      0.329,
						IcmpType: 3,
						IcmpCode: 0,
						IcmpQTos: 0,
						IcmpQTTL: 1,
					}},
				},
				{
					Tx:      parserTRC.TS{Sec: 1567900909, Usec: 730987},
					Replyc:  1,
					TTL:     2,
					Attempt: 0,
					Flowid:  4,
					Replies: []parserTRC.Reply{{
						Rx:       parserTRC.TS{Sec: 1567900909, Usec: 731554},
						TTL:      63,
						RTT:      0.567,
						IcmpType: 3,
						IcmpCode: 0,
						IcmpQTos: 0,
						IcmpQTTL: 1,
					}},
				},
				{
					Tx:      parserTRC.TS{Sec: 1567900909, Usec: 982029},
					Replyc:  1,
					TTL:     2,
					Attempt: 0,
					Flowid:  5,
					Replies: []parserTRC.Reply{{
						Rx:       parserTRC.TS{Sec: 1567900909, Usec: 982358},
						TTL:      63,
						RTT:      0.329,
						IcmpType: 3,
						IcmpCode: 0,
						IcmpQTos: 0,
						IcmpQTTL: 1,
					}},
				},
				{
					Tx:      parserTRC.TS{Sec: 1567900910, Usec: 232994},
					Replyc:  1,
					TTL:     2,
					Attempt: 0,
					Flowid:  6,
					Replies: []parserTRC.Reply{{
						Rx:       parserTRC.TS{Sec: 1567900910, Usec: 234231},
						TTL:      63,
						RTT:      1.237,
						IcmpType: 3,
						IcmpCode: 0,
						IcmpQTos: 0,
						IcmpQTTL: 1,
					}},
				},
			},
		}},
	}
	bqScamperLinkArray2 := make([]schema.BQScamperLinkArray, 2)
	bqScamperLinkArray2[0] = schema.BQScamperLinkArray{
		Links: []parserTRC.ScamperLink{
			{
				Addr: "*",
			},
		},
	}
	bqScamperLinkArray2[1] = schema.BQScamperLinkArray{
		Links: []parserTRC.ScamperLink{
			{
				Addr: "*",
			},
		},
	}

	cycleStartTime := float64(1566691268)
	bqScamperNode1 := schema.BQScamperNode{
		HopID: parser.GetHopID(cycleStartTime, "ndt-plh7v", "2001:550:1b01:1::1"),
		Addr:  "2001:550:1b01:1::1",
		Name:  "",
		QTTL:  1,
		Linkc: 1,
		Links: bqScamperLinkArray1,
	}
	bqScamperNode2 := schema.BQScamperNode{
		HopID: parser.GetHopID(cycleStartTime, "ndt-plh7v", "2001:4888:3f:6092:3a2:26:0:1"),
		Addr:  "2001:4888:3f:6092:3a2:26:0:1",
		Name:  "",
		QTTL:  1,
		Linkc: 1,
		Links: bqScamperLinkArray2,
	}

	bqScamperNodes := make([]schema.BQScamperNode, 2)
	bqScamperNodes[0] = bqScamperNode1
	bqScamperNodes[1] = bqScamperNode2

	bqTracelbLine := schema.BQTracelbLine{
		Type:        "tracelb",
		Version:     "0.1",
		Userid:      0,
		Method:      "icmp-echo",
		Src:         "2001:550:1b01:1:e41d:2d00:151:f6c0",
		Dst:         "2600:1009:b013:1a59:c369:b528:98fd:ab43",
		Start:       parserTRC.TS{Sec: 1567900908, Usec: 729543},
		ProbeSize:   60,
		Firsthop:    1,
		Attempts:    3,
		Confidence:  95,
		Tos:         0,
		Gaplimit:    0,
		WaitTimeout: 5,
		WaitProbe:   250,
		Probec:      85,
		ProbecMax:   3000,
		Nodec:       6,
		Linkc:       6,
		Nodes:       bqScamperNodes,
	}

	bqScamperOutput := schema.BQScamperOutput{
		Metadata: tracer.Metadata{
			UUID:                    "0000000000",
			TracerouteCallerVersion: "0000000",
			CachedResult:            false,
			CachedUUID:              "",
		},
		CycleStart: parserTRC.CyclestartLine{
			Type:      "cycle-start",
			ListName:  "/tmp/scamperctrl:51803",
			ID:        1,
			Hostname:  "ndt-plh7v",
			StartTime: cycleStartTime,
		},
		Tracelb: bqTracelbLine,
		CycleStop: parserTRC.CyclestopLine{
			Type:     "cycle-stop",
			ListName: "/tmp/scamperctrl:51803",
			ID:       1,
			Hostname: "ndt-plh7v",
			StopTime: 1566691541,
		},
	}

	return schema.Scamper1Row{
		ID:     "0000000000",
		Parser: parseInfo,
		Date:   date,
		Raw:    bqScamperOutput,
	}
}
