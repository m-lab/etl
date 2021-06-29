package parser_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

func TestExtractLogtimeFromFilename(t *testing.T) {
	log_time, _ := parser.ExtractLogtimeFromFilename("20170315T01:00:00Z_173.205.3.39_0.web100")
	if log_time.Unix() != 1489539600 {
		fmt.Println(log_time.Unix())
		t.Fatalf("log time not parsed correctly.")
	}
}

func TestPopulateSnap(t *testing.T) {
	ss_value := make(map[string]string)
	ss_value["CERcvd"] = "22"
	ss_value["RemAddress"] = "abcd"
	ss_value["TimeStampRcvd"] = "0"
	ss_value["StartTimeStamp"] = "2222"
	ss_value["StartTimeUsec"] = "1111"
	snap, err := parser.PopulateSnap(ss_value)
	if err != nil {
		t.Fatalf("Snap fields not populated correctly.")
	}

	if snap.TimeStampRcvd {
		t.Errorf("TimeStampRcvd; got %t; want false", snap.TimeStampRcvd)
	}
	if snap.RemAddress != "abcd" {
		t.Errorf("RemAddress; got %q; want 'abcd'", snap.RemAddress)
	}
	if snap.CERcvd != 22 {
		t.Errorf("CERcvd; got %d; want 22", snap.CERcvd)
	}
	// Verify StartTimeStamp is combined correctly with StartTimeUsec.
	if snap.StartTimeStamp != 2222001111 {
		t.Errorf("StartTimeStamp; got %d; want 222001111", snap.StartTimeStamp)
	}
}

func TestParseOneLine(t *testing.T) {
	header := "K: cid PollTime LocalAddress LocalPort RemAddress RemPort State SACKEnabled TimestampsEnabled NagleEnabled ECNEnabled SndWinScale RcvWinScale ActiveOpen MSSRcvd WinScaleRcvd WinScaleSent PktsOut DataPktsOut DataBytesOut PktsIn DataPktsIn DataBytesIn SndUna SndNxt SndMax ThruBytesAcked SndISS RcvNxt ThruBytesReceived RecvISS StartTimeSec StartTimeUsec Duration SndLimTransSender SndLimBytesSender SndLimTimeSender SndLimTransCwnd SndLimBytesCwnd SndLimTimeCwnd SndLimTransRwin SndLimBytesRwin SndLimTimeRwin SlowStart CongAvoid CongestionSignals OtherReductions X_OtherReductionsCV X_OtherReductionsCM CongestionOverCount CurCwnd MaxCwnd CurSsthresh LimCwnd MaxSsthresh MinSsthresh FastRetran Timeouts SubsequentTimeouts CurTimeoutCount AbruptTimeouts PktsRetrans BytesRetrans DupAcksIn SACKsRcvd SACKBlocksRcvd PreCongSumCwnd PreCongSumRTT PostCongSumRTT PostCongCountRTT ECERcvd SendStall QuenchRcvd RetranThresh NonRecovDA AckAfterFR DSACKDups SampleRTT SmoothedRTT RTTVar MaxRTT MinRTT SumRTT CountRTT CurRTO MaxRTO MinRTO CurMSS MaxMSS MinMSS X_Sndbuf X_Rcvbuf CurRetxQueue MaxRetxQueue CurAppWQueue MaxAppWQueue CurRwinSent MaxRwinSent MinRwinSent LimRwin DupAcksOut CurReasmQueue MaxReasmQueue CurAppRQueue MaxAppRQueue X_rcv_ssthresh X_wnd_clamp X_dbg1 X_dbg2 X_dbg3 X_dbg4 CurRwinRcvd MaxRwinRcvd MinRwinRcvd LocalAddressType X_RcvRTT WAD_IFQ WAD_MaxBurst WAD_MaxSsthresh WAD_NoAI WAD_CwndAdjust"
	var_names, err := parser.ParseKHeader(header)
	if err != nil {
		t.Fatalf("Do not parse header correctly.")
	}
	oneLine := "C: 21605 2017-02-03-12:00:03Z 213.248.112.75 41131 5.228.253.100 52290 1 3 0 1 0 8 7 0 0 8 7 6184 6184 123680 11116 11115 16187392 3492237027 3492237027 3492237027 1 3492237026 1028482265 16187392 1012294873 1486123188 191060 14839426 1 123680 13442498 0 0 0 0 0 0 1 0 0 0 0 0 0 5840 5840 4294966680 4294965836 0 4294967295 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 3 0 0 0 72 72 50 72 72 72 1 272 272 272 1460 1460 1460 16384 4194304 0 0 0 0 3145728 3145728 5840 8365440 0 0 0 0 13140 3147040 4287744 3145728 1460 3145728 0 65536 65536 65536 1 269387 0 0 0 0 0"
	ss_value, err := parser.ParseOneLine(oneLine, var_names)
	if err != nil {
		t.Fatalf("The content parsing not completed.")
	}
	if len(ss_value) != 121 || ss_value["SampleRTT"] != "72" {
		t.Fatalf("The content not parsed correctly.")
	}
}

func TestSSInserter(t *testing.T) {
	ins := &inMemoryInserter{}
	// Completely fake annotation data.
	responseJSON := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
		"Annotations":{"5.228.253.100":{"Geo":{"postal_code":"52282"}, "Network":{"Systems":[{"ASNs":[456]}]}},
				   "178.141.112.12":{"Geo":{"postal_code":"17814"}, "Network":{"Systems":[{"ASNs":[456]}]}},
				   "193.169.96.33":{"Geo":{"postal_code":"19316"}, "Network":{"Systems":[{"ASNs":[456]}]}},
				   "178.141.112.12":{"Geo":{"postal_code":"17814"}, "Network":{"Systems":[{"ASNs":[456]}]}},
				   "45.56.98.222":{"Geo":{"postal_code":"45569"}, "Network":{"Systems":[{"ASNs":[456]}]}},
				   "213.248.112.75":{"Geo":{"postal_code":"213248"}, "Network":{"Systems":[{"ASNs":[456]}]}}
				   }}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, responseJSON)
	}))
	defer ts.Close()

	p := parser.NewSSParser(ins, v2as.GetAnnotator(ts.URL))
	filename := "testdata/sidestream/20170203T00:00:00Z_ALL0.web100"
	rawData, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}

	meta := map[string]bigquery.Value{"filename": filename}
	err = p.ParseAndInsert(meta, filename, rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = p.Annotate(p.TableName())
	if err != nil {
		t.Error(err)
	}
	err = p.Flush()
	if err != nil {
		t.Error(err)
	}
	if ins.Committed() != 6 {
		t.Fatalf("Expected %d, Got %d.", 6, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}

	for _, r := range ins.data {
		row, _ := r.(*schema.SS)
		if row.Web100_log_entry.Connection_spec.Remote_geolocation.PostalCode == "" {
			t.Error(row.Web100_log_entry.Connection_spec.Remote_ip, "missing PostalCode")
		}
	}
	inserted := ins.data[0].(*schema.SS)
	if inserted.ParseTime.After(time.Now()) {
		t.Error("Should have inserted parse_time")
	}
	if inserted.TaskFileName != filename {
		t.Error("Should have correct filename", filename, "!=", inserted.TaskFileName)
	}

	if inserted.ParserVersion != "https://github.com/m-lab/etl/tree/foobar" {
		t.Error("ParserVersion not properly set")
	}
	// echo -n testdata/sidestream/20170203T00:00:00Z_ALL0.web100-1486123188191060-213.248.112.75-41131-5.228.253.100-52290 | \
	//     openssl dgst -binary -md5 | base64  | tr '/+' '_-' | tr -d '='
	if inserted.ID != "cjFOd7-tIa3RXxWMhCNSrQ" {
		t.Errorf("ss.ParseAndInsert() wrong ID; got %q, want %q", inserted.ID, "cjFOd7-tIa3RXxWMhCNSrQ")
	}
}
