package parser_test

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/go-test/deep"

	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/uuid-annotator/annotator"
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
	p := parser.NewSSParser(ins)
	filename := "testdata/sidestream/20170203T00:00:00Z_ALL0.web100"
	rawData, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}

	taskFilename := "gs://archive-measurement-lab/sidestream/2019/11/20/20191120T010010Z-mlab1-ord03-sidestream-0000.tgz"

	meta := map[string]bigquery.Value{"filename": taskFilename}
	err = p.ParseAndInsert(meta, filename, rawData)
	if err != nil {
		t.Fatalf(err.Error())
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

	inserted := ins.data[0].(*schema.SS)
	if inserted.ParseTime.After(time.Now()) {
		t.Error("Should have inserted parse_time")
	}
	if inserted.TaskFileName != taskFilename {
		t.Error("Should have correct filename", taskFilename, "!=", inserted.TaskFileName)
	}

	if inserted.ParserVersion != "https://github.com/m-lab/etl/tree/foobar" {
		t.Error("ParserVersion not properly set")
	}
	// echo -n testdata/sidestream/20170203T00:00:00Z_ALL0.web100-1486123188191060-213.248.112.75-41131-5.228.253.100-52290 | \
	//     openssl dgst -binary -md5 | base64  | tr '/+' '_-' | tr -d '='
	if inserted.ID != "cjFOd7-tIa3RXxWMhCNSrQ" {
		t.Errorf("ss.ParseAndInsert() wrong ID; got %q, want %q", inserted.ID, "cjFOd7-tIa3RXxWMhCNSrQ")
	}

	// The expected connection spec includes all legacy fields as well as fully
	// populated ServerX and ClientX fields.
	expectedSpec := schema.Web100ConnectionSpecification{
		Local_ip:    "213.248.112.75",
		Local_af:    2,
		Local_port:  41131,
		Remote_ip:   "5.228.253.100",
		Remote_port: 52290,
		ServerX: annotator.ServerAnnotations{
			Site:    "ord03",
			Machine: "mlab1",
		},
	}

	if diff := deep.Equal(inserted.Web100_log_entry.Connection_spec, expectedSpec); diff != nil {
		t.Error("Connection spec does not match:", diff)
	}
}
