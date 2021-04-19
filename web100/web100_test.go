package web100_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"

	pipe "gopkg.in/m-lab/pipe.v3"

	"github.com/m-lab/etl/web100"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestHeaderParsing(t *testing.T) {
	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/web100/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	slog, err := web100.NewSnapLog(c2sData)

	if err != nil {
		t.Fatal(err.Error())
	}
	if slog.SnapshotNumFields() != 142 {
		log.Printf("%d\n", slog.SnapshotNumFields())
		t.Error("Wrong number of fields.")
	}
	if slog.SnapshotNumBytes() != 669 {
		log.Printf("Record length %d\n", slog.SnapshotNumBytes())
		t.Error("Wrong record length.")
	}

	if slog.LogTime != 1494337516 {
		t.Error("Incorrect LogTime.")
	}
	if err = slog.ValidateSnapshots(); err != nil {
		t.Error(err)
	}
}

type SimpleSaver struct {
	Integers map[string]int64
	Strings  map[string]string
	Bools    map[string]bool
}

func NewSimpleSaver() SimpleSaver {
	return SimpleSaver{make(map[string]int64, 10),
		make(map[string]string, 10), make(map[string]bool, 10)}
}

func (s SimpleSaver) SetString(name string, val string) {
	s.Strings[name] = val
}

func (s SimpleSaver) SetInt64(name string, val int64) {
	s.Integers[name] = val
}

func (s SimpleSaver) SetBool(name string, val bool) {
	s.Bools[name] = val
}

// These json blobs were created using the old, C web100 based parser.
var old1 = `{"Integers":{"AbruptTimeouts":0,"ActiveOpen":0,"CERcvd":0,"CongAvoid":2,"CongOverCount":0,"CongSignals":0,"CountRTT":3,"CurAppRQueue":297,"CurAppWQueue":0,"CurCwnd":4344,"CurMSS":1448,"CurRTO":688,"CurReasmQueue":0,"CurRetxQueue":0,"CurRwinRcvd":29312,"CurRwinSent":6912,"CurSsthresh":2896,"CurTimeoutCount":0,"DSACKDups":0,"DataSegsIn":1,"DataSegsOut":3,"DupAcksIn":0,"DupAcksOut":0,"Duration":2343340,"ECN":0,"FastRetran":0,"HCDataOctetsIn":297,"HCDataOctetsOut":254,"HCThruOctetsAcked":158,"HCThruOctetsReceived":297,"LimCwnd":4294965848,"LimRwin":8365440,"LocalAddressType":1,"LocalPort":46024,"MSSRcvd":0,"MaxAppRQueue":297,"MaxAppWQueue":0,"MaxMSS":1448,"MaxRTO":738,"MaxRTT":244,"MaxReasmQueue":0,"MaxRetxQueue":0,"MaxRwinRcvd":29312,"MaxRwinSent":6912,"MaxSsCwnd":4344,"MaxSsthresh":2896,"MinMSS":1448,"MinRTO":687,"MinRTT":229,"MinRwinRcvd":29312,"MinRwinSent":5792,"MinSsthresh":2896,"Nagle":1,"NonRecovDA":0,"OctetsRetrans":0,"OtherReductions":0,"PostCongCountRTT":0,"PostCongSumRTT":0,"PreCongSumCwnd":0,"PreCongSumRTT":0,"QuenchRcvd":0,"RTTVar":110,"RcvNxt":3198753442,"RcvRTT":0,"RcvWindScale":7,"RecInitial":3198753145,"RemPort":48716,"RetranThresh":3,"SACK":3,"SACKBlocksRcvd":0,"SACKsRcvd":0,"SampleRTT":244,"SegsIn":3,"SegsOut":3,"SegsRetrans":0,"SendStall":0,"SlowStart":0,"SmoothedRTT":246,"SndInitial":2301393414,"SndLimBytesCwnd":0,"SndLimBytesRwin":0,"SndLimBytesSender":254,"SndLimTimeCwnd":0,"SndLimTimeRwin":0,"SndLimTimeSnd":234061,"SndLimTransCwnd":0,"SndLimTransRwin":0,"SndLimTransSnd":1,"SndMax":2301393572,"SndNxt":2301393572,"SndUna":2301393572,"SndWindScale":7,"SpuriousFrDetected":0,"StartTimeStamp":1494337514,"StartTimeUsec":369834,"State":5,"SubsequentTimeouts":0,"SumRTT":707,"TimeStamps":1,"Timeouts":0,"WinScaleRcvd":7,"WinScaleSent":7,"X_OtherReductionsCM":0,"X_OtherReductionsCV":0,"X_Rcvbuf":87380,"X_Sndbuf":16384,"X_dbg1":6912,"X_dbg2":536,"X_dbg3":6864,"X_dbg4":0,"X_rcv_ssthresh":6864,"X_wnd_clamp":64087},"Strings":{"LocalAddress":"213.208.152.37","RemAddress":"45.56.98.222"}, "Bools":{}}`

var old1000 = `{"Integers":{"AbruptTimeouts":0,"ActiveOpen":0,"CERcvd":0,"CongAvoid":2,"CongOverCount":0,"CongSignals":0,"CountRTT":3,"CurAppRQueue":0,"CurAppWQueue":0,"CurCwnd":4344,"CurMSS":1448,"CurRTO":688,"CurReasmQueue":0,"CurRetxQueue":0,"CurRwinRcvd":29312,"CurRwinSent":64128,"CurSsthresh":2896,"CurTimeoutCount":0,"DSACKDups":0,"DataSegsIn":254,"DataSegsOut":141,"DupAcksIn":0,"DupAcksOut":0,"Duration":7519783,"ECN":0,"FastRetran":0,"HCDataOctetsIn":365207,"HCDataOctetsOut":4670,"HCThruOctetsAcked":158,"HCThruOctetsReceived":365207,"LimCwnd":4294965848,"LimRwin":8365440,"LocalAddressType":1,"LocalPort":46024,"MSSRcvd":0,"MaxAppRQueue":2896,"MaxAppWQueue":0,"MaxMSS":1448,"MaxRTO":738,"MaxRTT":244,"MaxReasmQueue":0,"MaxRetxQueue":0,"MaxRwinRcvd":29312,"MaxRwinSent":64128,"MaxSsCwnd":4344,"MaxSsthresh":2896,"MinMSS":1448,"MinRTO":687,"MinRTT":229,"MinRwinRcvd":29312,"MinRwinSent":5792,"MinSsthresh":2896,"Nagle":1,"NonRecovDA":0,"OctetsRetrans":0,"OtherReductions":0,"PostCongCountRTT":0,"PostCongSumRTT":0,"PreCongSumCwnd":0,"PreCongSumRTT":0,"QuenchRcvd":0,"RTTVar":110,"RcvNxt":3199118352,"RcvRTT":175000,"RcvWindScale":7,"RecInitial":3198753145,"RemPort":48716,"RetranThresh":3,"SACK":3,"SACKBlocksRcvd":0,"SACKsRcvd":0,"SampleRTT":244,"SegsIn":256,"SegsOut":141,"SegsRetrans":0,"SendStall":0,"SlowStart":0,"SmoothedRTT":246,"SndInitial":2301393414,"SndLimBytesCwnd":0,"SndLimBytesRwin":0,"SndLimBytesSender":254,"SndLimTimeCwnd":0,"SndLimTimeRwin":0,"SndLimTimeSnd":234061,"SndLimTransCwnd":0,"SndLimTransRwin":0,"SndLimTransSnd":1,"SndMax":2301393572,"SndNxt":2301393572,"SndUna":2301393572,"SndWindScale":7,"SpuriousFrDetected":0,"StartTimeStamp":1494337514,"StartTimeUsec":369834,"State":5,"SubsequentTimeouts":0,"SumRTT":707,"TimeStamps":1,"Timeouts":0,"WinScaleRcvd":7,"WinScaleSent":7,"X_OtherReductionsCM":0,"X_OtherReductionsCV":0,"X_Rcvbuf":90112,"X_Sndbuf":16384,"X_dbg1":64128,"X_dbg2":1448,"X_dbg3":64087,"X_dbg4":0,"X_rcv_ssthresh":64087,"X_wnd_clamp":63712},"Strings":{"LocalAddress":"213.208.152.37","RemAddress":"45.56.98.222"}, "Bools":{}}`

var old2000 = `{"Integers":{"AbruptTimeouts":0,"ActiveOpen":0,"CERcvd":0,"CongAvoid":2,"CongOverCount":0,"CongSignals":0,"CountRTT":3,"CurAppRQueue":0,"CurAppWQueue":0,"CurCwnd":4344,"CurMSS":1448,"CurRTO":688,"CurReasmQueue":0,"CurRetxQueue":0,"CurRwinRcvd":29312,"CurRwinSent":104320,"CurSsthresh":2896,"CurTimeoutCount":0,"DSACKDups":0,"DataSegsIn":1237,"DataSegsOut":639,"DupAcksIn":0,"DupAcksOut":0,"Duration":12709989,"ECN":0,"FastRetran":0,"HCDataOctetsIn":1788591,"HCDataOctetsOut":20606,"HCThruOctetsAcked":158,"HCThruOctetsReceived":1788591,"LimCwnd":4294965848,"LimRwin":8365440,"LocalAddressType":1,"LocalPort":46024,"MSSRcvd":0,"MaxAppRQueue":2896,"MaxAppWQueue":0,"MaxMSS":1448,"MaxRTO":738,"MaxRTT":244,"MaxReasmQueue":0,"MaxRetxQueue":0,"MaxRwinRcvd":29312,"MaxRwinSent":104320,"MaxSsCwnd":4344,"MaxSsthresh":2896,"MinMSS":1448,"MinRTO":687,"MinRTT":229,"MinRwinRcvd":29312,"MinRwinSent":5792,"MinSsthresh":2896,"Nagle":1,"NonRecovDA":0,"OctetsRetrans":0,"OtherReductions":0,"PostCongCountRTT":0,"PostCongSumRTT":0,"PreCongSumCwnd":0,"PreCongSumRTT":0,"QuenchRcvd":0,"RTTVar":110,"RcvNxt":3200541736,"RcvRTT":130375,"RcvWindScale":7,"RecInitial":3198753145,"RemPort":48716,"RetranThresh":3,"SACK":3,"SACKBlocksRcvd":0,"SACKsRcvd":0,"SampleRTT":244,"SegsIn":1239,"SegsOut":639,"SegsRetrans":0,"SendStall":0,"SlowStart":0,"SmoothedRTT":246,"SndInitial":2301393414,"SndLimBytesCwnd":0,"SndLimBytesRwin":0,"SndLimBytesSender":254,"SndLimTimeCwnd":0,"SndLimTimeRwin":0,"SndLimTimeSnd":234061,"SndLimTransCwnd":0,"SndLimTransRwin":0,"SndLimTransSnd":1,"SndMax":2301393572,"SndNxt":2301393572,"SndUna":2301393572,"SndWindScale":7,"SpuriousFrDetected":0,"StartTimeStamp":1494337514,"StartTimeUsec":369834,"State":5,"SubsequentTimeouts":0,"SumRTT":707,"TimeStamps":1,"Timeouts":0,"WinScaleRcvd":7,"WinScaleSent":7,"X_OtherReductionsCM":0,"X_OtherReductionsCV":0,"X_Rcvbuf":147456,"X_Sndbuf":16384,"X_dbg1":104320,"X_dbg2":1448,"X_dbg3":104256,"X_dbg4":0,"X_rcv_ssthresh":104256,"X_wnd_clamp":104256},"Strings":{"LocalAddress":"213.208.152.37","RemAddress":"45.56.98.222"}, "Bools":{}}`

// This tests parsing of snapshot content for three snapshots.
func TestSnapshotContent(t *testing.T) {
	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/web100/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}
	slog, err := web100.NewSnapLog(c2sData)
	if err != nil {
		t.Fatal(err.Error())
	}

	saver := NewSimpleSaver()

	var old SimpleSaver

	json.Unmarshal([]byte(old1), &old)
	snapshot, _ := slog.Snapshot(1)
	snapshot.SnapshotValues(&saver)
	if !reflect.DeepEqual(old, saver) {
		t.Error("Does not match old output")
		fmt.Printf("%d %d\n", old.Integers["Duration"], saver.Integers["Duration"])
		fmt.Printf("%+v\n", saver)
		fmt.Printf("%+v\n", old)
	}

	json.Unmarshal([]byte(old1000), &old)
	snapshot, _ = slog.Snapshot(1000)
	snapshot.SnapshotValues(&saver)
	if !reflect.DeepEqual(old, saver) {
		t.Error("Does not match old output")
		fmt.Printf("%d %d\n", old.Integers["Duration"], saver.Integers["Duration"])
		fmt.Printf("%+v\n", saver)
		fmt.Printf("%+v\n", old)
	}

	json.Unmarshal([]byte(old2000), &old)
	snapshot, _ = slog.Snapshot(2000)
	snapshot.SnapshotValues(&saver)
	if !reflect.DeepEqual(old, saver) {
		t.Error("Does not match old output")
		fmt.Printf("%d %d\n", old.Integers["Duration"], saver.Integers["Duration"])
		fmt.Printf("%+v\n", saver)
		fmt.Printf("%+v\n", old)
	}
}

// The remaining tests just verify that the parser produces valid snapshots.  They
// do not verify the content accuracy.
func OneSnapshot(t *testing.T, name string, n int) {
	data, err := ioutil.ReadFile(`testdata/web100/` + name)
	if err != nil {
		t.Fatalf(err.Error())
	}
	slog, err := web100.NewSnapLog(data)
	if err != nil {
		t.Fatal(err.Error())
	}

	saver := NewSimpleSaver()

	snapshot, err := slog.Snapshot(n)
	if err != nil {
		t.Fatal(err.Error())
	}
	snapshot.SnapshotValues(&saver)
	if len(saver.Integers) != 112 {
		t.Fatal("Incorrect number of integers: ", len(saver.Integers))
	}
	if len(saver.Strings) != 2 {
		t.Fatal("Incorrect number of strings: ", len(saver.Strings))
	}
}

// These files are in a different format, so don't try to parse them.
func TestSnapshot200903(t *testing.T) {
	t.Skip("Different format - skip test")
	OneSnapshot(t, "20090301T22:29:43.653205000Z-78.61.75.41:33538.s2c_snaplog", 2000)
	OneSnapshot(t, "20090301T22:29:43.653205000Z_78.61.75.41:46267.c2s_snaplog", 2000)
}

func TestSnapshot200904(t *testing.T) {
	OneSnapshot(t, "20090401T09:01:09.490730000Z-131.169.137.246:14884.s2c_snaplog", 2000)
	OneSnapshot(t, "20090401T09:01:09.490730000Z_131.169.137.246:14881.c2s_snaplog", 2000)
}

func TestSnapshot200906(t *testing.T) {
	OneSnapshot(t, "20090601T22:19:19.325928000Z_75.133.69.98:60630.c2s_snaplog", 2000)
	OneSnapshot(t, "20090601T22:19:19.325928000Z-75.133.69.98:60631.s2c_snaplog", 2000)
}

func TestSnapshot201704(t *testing.T) {
	OneSnapshot(t,
		"20170430T11:54:26.658288000Z_p508486E9.dip0.t-ipconnect.de:53087.c2s_snaplog",
		1900)
	OneSnapshot(t,
		"20170430T11:54:26.658288000Z_p508486E9.dip0.t-ipconnect.de:53088.s2c_snaplog",
		1900)
}

func TestNewVar(t *testing.T) {
	_, err := web100.NewVariable("foo 1 1 1")
	if err == nil {
		t.Error("Should have returned error")
	}
	// An INTEGER32 type
	v, err := web100.NewVariable("foo 0 1 4")
	if err != nil {
		t.Error(err.Error())
	}
	saver := NewSimpleSaver()
	v.Save([]byte{1, 2, 3, 4}, saver)
	if saver.Integers["foo"] != 0x04030201 {
		t.Error(fmt.Sprintf("Actual: %x", saver.Integers["foo"]))
	}
	v.Save([]byte{0xff, 0xff, 0xff, 0xff}, saver)
	if saver.Integers["foo"] != -1 {
		t.Error(fmt.Sprintf("Actual: %x", saver.Integers["foo"]))
	}

	//	4 /*INTEGER*/, 4 /*INTEGER32*/, 4 /*IPV4*/, 4 /*COUNTER32*/, 4, /*GAUGE32*/
	//	4 /*UNSIGNED32*/, 4, /*TIME_TICKS*/
	//	8 /*COUNTER64*/, 2 /*PORT_NUM*/, 17, 17, 32 /*STR32*/, 1 /*OCTET*/, 0}
}

func TestChangeIndices(t *testing.T) {
	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/web100/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	slog, _ := web100.NewSnapLog(c2sData)

	x, err := slog.ChangeIndices("CongestionSignals")
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Printf("count: %d\n", len(x))
}

// About 70 usec per test, independent of which field is used.
func BenchmarkChangeIndices(b *testing.B) {
	b.StopTimer()
	s2cName := `20090601T22:19:19.325928000Z-75.133.69.98:60631.s2c_snaplog`
	data, err := ioutil.ReadFile(`testdata/web100/` + s2cName)
	if err != nil {
		b.Fatalf(err.Error())
	}

	slog, _ := web100.NewSnapLog(data)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := slog.ChangeIndices("SmoothedRTT")
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

// 3 usec for 78 items
// 100 nsec for zero items, and about 40 nsec per index.
func BenchmarkSliceInt(b *testing.B) {
	b.StopTimer()
	//s2cName := `20090401T09:01:09.490730000Z_131.169.137.246:14881.c2s_snaplog`
	//s2cName := `20170430T11:54:26.658288000Z_p508486E9.dip0.t-ipconnect.de:53088.s2c_snaplog`
	s2cName := `20090601T22:19:19.325928000Z-75.133.69.98:60631.s2c_snaplog`
	data, err := ioutil.ReadFile(`testdata/web100/` + s2cName)
	if err != nil {
		b.Fatalf(err.Error())
	}
	slog, err := web100.NewSnapLog(data)
	if err != nil {
		b.Fatalf(err.Error())
	}
	indices, err := slog.ChangeIndices("SmoothedRTT")
	if err != nil {
		b.Fatalf(err.Error())
	}
	log.Println(len(indices))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		slog.SliceIntField("SmoothedRTT", indices)
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

type NullSaver struct{}

func (s *NullSaver) SetString(name string, val string) {}
func (s *NullSaver) SetInt64(name string, val int64)   {}
func (s *NullSaver) SetBool(name string, val bool)     {}

// 30 nsec/op, 0 allocs/op
func BenchmarkSaver(b *testing.B) {
	ns := NullSaver{}
	v, _ := web100.NewVariable("SmoothedRTT 0 4 4")
	data := []byte{0, 1, 2, 3, 4, 5, 6}
	for i := 0; i < b.N; i++ {
		v.Save(data, &ns)
	}
}

func TestMain(m *testing.M) {
	p := pipe.Script(
		"unpacking testdata files",
		pipe.Exec("tar", "-C", "testdata", "-xvf", "testdata/web100-files.tar.gz"),
	)
	_, err := pipe.CombinedOutput(p)
	if err != nil {
		log.Fatal(err)
	}
	exitCode := m.Run()
	os.RemoveAll("testdata/web100")
	os.Exit(exitCode)
}
