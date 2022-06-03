// This files contains schema for SideStream tests.
// Any changes here should also be made in ss.json
package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/uuid-annotator/annotator"
)

type Web100ConnectionSpecification struct {
	Local_ip           string            `json:"local_ip" bigquery:"local_ip"`
	Local_af           int64             `json:"local_af" bigquery:"local_af"`
	Local_port         int64             `json:"local_port" bigquery:"local_port"`
	Remote_ip          string            `json:"remote_ip" bigquery:"remote_ip"`
	Remote_port        int64             `json:"remote_port" bigquery:"remote_port"`
	Local_geolocation  api.GeolocationIP `json:"local_geolocation" bigquery:"local_geolocation"`
	Remote_geolocation api.GeolocationIP `json:"remote_geolocation" bigquery:"remote_geolocation"`

	// ServerX and ClientX are for the synthetic UUID annotator export process.
	ServerX annotator.ServerAnnotations
	ClientX annotator.ClientAnnotations
}

type Web100Snap struct {
	AbruptTimeouts       int64  `json:"AbruptTimeouts"`
	ActiveOpen           int64  `json:"ActiveOpen"`
	CERcvd               int64  `json:"CERcvd"`
	CongAvoid            int64  `json:"CongAvoid"`
	CongOverCount        int64  `json:"CongOverCount"`
	CongSignals          int64  `json:"CongSignals"`
	CountRTT             int64  `json:"CountRTT"`
	CurAppRQueue         int64  `json:"CurAppRQueue"`
	CurAppWQueue         int64  `json:"CurAppWQueue"`
	CurCwnd              int64  `json:"CurCwnd"`
	CurMSS               int64  `json:"CurMSS"`
	CurRTO               int64  `json:"CurRTO"`
	CurReasmQueue        int64  `json:"CurReasmQueue"`
	CurRetxQueue         int64  `json:"CurRetxQueue"`
	CurRwinRcvd          int64  `json:"CurRwinRcvd"`
	CurRwinSent          int64  `json:"CurRwinSent"`
	CurSsthresh          int64  `json:"CurSsthresh"`
	CurTimeoutCount      int64  `json:"CurTimeoutCount"`
	DSACKDups            int64  `json:"DSACKDups"`
	DataOctetsIn         int64  `json:"DataOctetsIn"`
	DataOctetsOut        int64  `json:"DataOctetsOut"`
	DataSegsIn           int64  `json:"DataSegsIn"`
	DataSegsOut          int64  `json:"DataSegsOut"`
	DupAckEpisodes       int64  `json:"DupAckEpisodes"`
	DupAcksIn            int64  `json:"DupAcksIn"`
	DupAcksOut           int64  `json:"DupAcksOut"`
	Duration             int64  `json:"Duration"`
	ECESent              int64  `json:"ECESent"`
	ECN                  int64  `json:"ECN"`
	ECNNonceRcvd         int64  `json:"ECNNonceRcvd"`
	ECNsignals           int64  `json:"ECNsignals"`
	ElapsedMicroSecs     int64  `json:"ElapsedMicroSecs"`
	ElapsedSecs          int64  `json:"ElapsedSecs"`
	FastRetran           int64  `json:"FastRetran"`
	HCDataOctetsIn       int64  `json:"HCDataOctetsIn"`
	HCDataOctetsOut      int64  `json:"HCDataOctetsOut"`
	HCSumRTT             int64  `json:"HCSumRTT"`
	HCThruOctetsAcked    int64  `json:"HCThruOctetsAcked"`
	HCThruOctetsReceived int64  `json:"HCThruOctetsReceived"`
	InRecovery           int64  `json:"InRecovery"`
	IpTosIn              int64  `json:"IpTosIn"`
	IpTosOut             int64  `json:"IpTosOut"`
	IpTtl                int64  `json:"IpTtl"`
	LimCwnd              int64  `json:"LimCwnd"`
	LimMSS               int64  `json:"LimMSS"`
	LimRwin              int64  `json:"LimRwin"`
	LimSsthresh          int64  `json:"LimSsthresh"`
	LocalAddress         string `json:"LocalAddress"`
	LocalAddressType     int64  `json:"LocalAddressType"`
	LocalPort            int64  `json:"LocalPort"`
	MSSRcvd              int64  `json:"MSSRcvd"`
	MSSSent              int64  `json:"MSSSent"`
	MaxAppRQueue         int64  `json:"MaxAppRQueue"`
	MaxAppWQueue         int64  `json:"MaxAppWQueue"`
	MaxCaCwnd            int64  `json:"MaxCaCwnd"`
	MaxMSS               int64  `json:"MaxMSS"`
	MaxPipeSize          int64  `json:"MaxPipeSize"`
	MaxRTO               int64  `json:"MaxRTO"`
	MaxRTT               int64  `json:"MaxRTT"`
	MaxReasmQueue        int64  `json:"MaxReasmQueue"`
	MaxRetxQueue         int64  `json:"MaxRetxQueue"`
	MaxRwinRcvd          int64  `json:"MaxRwinRcvd"`
	MaxRwinSent          int64  `json:"MaxRwinSent"`
	MaxSsCwnd            int64  `json:"MaxSsCwnd"`
	MaxSsthresh          int64  `json:"MaxSsthresh"`
	MinMSS               int64  `json:"MinMSS"`
	MinRTO               int64  `json:"MinRTO"`
	MinRTT               int64  `json:"MinRTT"`
	MinRwinRcvd          int64  `json:"MinRwinRcvd"`
	MinRwinSent          int64  `json:"MinRwinSent"`
	MinSsthresh          int64  `json:"MinSsthresh"`
	Nagle                int64  `json:"Nagle"`
	NonRecovDA           int64  `json:"NonRecovDA"`
	NonRecovDAEpisodes   int64  `json:"NonRecovDAEpisodes"`
	OctetsRetrans        int64  `json:"OctetsRetrans"`
	OtherReductions      int64  `json:"OtherReductions"`
	PipeSize             int64  `json:"PipeSize"`
	PostCongCountRTT     int64  `json:"PostCongCountRTT"`
	PostCongSumRTT       int64  `json:"PostCongSumRTT"`
	PreCongSumCwnd       int64  `json:"PreCongSumCwnd"`
	PreCongSumRTT        int64  `json:"PreCongSumRTT"`
	QuenchRcvd           int64  `json:"QuenchRcvd"`
	RTTVar               int64  `json:"RTTVar"`
	RcvNxt               int64  `json:"RcvNxt"`
	RcvRTT               int64  `json:"RcvRTT"`
	RcvWindScale         int64  `json:"RcvWindScale"`
	RecInitial           int64  `json:"RecInitial"`
	RemAddress           string `json:"RemAddress"`
	RemPort              int64  `json:"RemPort"`
	RetranThresh         int64  `json:"RetranThresh"`
	SACK                 int64  `json:"SACK"`
	SACKBlocksRcvd       int64  `json:"SACKBlocksRcvd"`
	SACKsRcvd            int64  `json:"SACKsRcvd"`
	SampleRTT            int64  `json:"SampleRTT"`
	SegsIn               int64  `json:"SegsIn"`
	SegsOut              int64  `json:"SegsOut"`
	SegsRetrans          int64  `json:"SegsRetrans"`
	SendStall            int64  `json:"SendStall"`
	SlowStart            int64  `json:"SlowStart"`
	SmoothedRTT          int64  `json:"SmoothedRTT"`
	SndInitial           int64  `json:"SndInitial"`
	SndLimBytesCwnd      int64  `json:"SndLimBytesCwnd"`
	SndLimBytesRwin      int64  `json:"SndLimBytesRwin"`
	SndLimBytesSender    int64  `json:"SndLimBytesSender"`
	SndLimTimeCwnd       int64  `json:"SndLimTimeCwnd"`
	SndLimTimeRwin       int64  `json:"SndLimTimeRwin"`
	SndLimTimeSnd        int64  `json:"SndLimTimeSnd"`
	SndLimTransCwnd      int64  `json:"SndLimTransCwnd"`
	SndLimTransRwin      int64  `json:"SndLimTransRwin"`
	SndLimTransSnd       int64  `json:"SndLimTransSnd"`
	SndMax               int64  `json:"SndMax"`
	SndNxt               int64  `json:"SndNxt"`
	SndUna               int64  `json:"SndUna"`
	SndWindScale         int64  `json:"SndWindScale"`
	SoftErrorReason      int64  `json:"SoftErrorReason"`
	SoftErrors           int64  `json:"SoftErrors"`
	SpuriousFrDetected   int64  `json:"SpuriousFrDetected"`
	SpuriousRtoDetected  int64  `json:"SpuriousRtoDetected"`
	StartTimeStamp       int64  `json:"StartTimeStamp"`
	State                int64  `json:"State"`
	SubsequentTimeouts   int64  `json:"SubsequentTimeouts"`
	SumOctetsReordered   int64  `json:"SumOctetsReordered"`
	SumRTT               int64  `json:"SumRTT"`
	ThruOctetsAcked      int64  `json:"ThruOctetsAcked"`
	ThruOctetsReceived   int64  `json:"ThruOctetsReceived"`
	TimeStamps           int64  `json:"TimeStamps"`
	TimeStampRcvd        bool   `json:"TimeStampRcvd"`
	TimeStampSent        bool   `json:"TimeStampSent"`
	Timeouts             int64  `json:"Timeouts"`
	WAD_CwndAdjust       int64  `json:"WAD_CwndAdjust"`
	WAD_IFQ              int64  `json:"WAD_IFQ"`
	WAD_MaxBurst         int64  `json:"WAD_MaxBurst"`
	WAD_MaxSsthresh      int64  `json:"WAD_MaxSsthresh"`
	WAD_NoAI             int64  `json:"WAD_NoAI"`
	WillSendSACK         int64  `json:"WillSendSACK"`
	WillUseSACK          int64  `json:"WillUseSACK"`
	WinScaleRcvd         int64  `json:"WinScaleRcvd"`
	WinScaleSent         int64  `json:"WinScaleSent"`
	X_OtherReductionsCM  int64  `json:"X_OtherReductionsCM"`
	X_OtherReductionsCV  int64  `json:"X_OtherReductionsCV"`
	X_Rcvbuf             int64  `json:"X_Rcvbuf"`
	X_Sndbuf             int64  `json:"X_Sndbuf"`
	X_dbg1               int64  `json:"X_dbg1"`
	X_dbg2               int64  `json:"X_dbg2"`
	X_dbg3               int64  `json:"X_dbg3"`
	X_dbg4               int64  `json:"X_dbg4"`
	X_rcv_ssthresh       int64  `json:"X_rcv_ssthresh"`
	X_wnd_clamp          int64  `json:"X_wnd_clamp"`
	ZeroRwinRcvd         int64  `json:"ZeroRwinRcvd"`
	ZeroRwinSent         int64  `json:"ZeroRwinSent"`
}

type Web100LogEntry struct {
	LogTime         int64                         `json:"log_time" bigquery:"log_time"`
	Version         string                        `json:"version" bigquery:"version"`
	Group_name      string                        `json:"group_name" bigquery:"group_name"`
	Connection_spec Web100ConnectionSpecification `json:"connection_spec" bigquery:"connection_spec"`
	Snap            Web100Snap                    `json:"snap" bigquery:"snap"`
}

type Anomalies struct {
	Exclusion_level int64 `json:"exclusion_level" bigquery:"exclusion_level"`
}

type SS struct {
	ID               string         `json:"id" bigquery:"id"`
	TestID           string         `json:"test_id" bigquery:"test_id"`
	Project          int64          `json:"project" bigquery:"project"`
	LogTime          int64          `json:"log_time" bigquery:"log_time"`
	ParseTime        time.Time      `json:"parse_time" bigquery:"parse_time"`
	ParserVersion    string         `json:"parser_version" bigquery:"parser_version"`
	TaskFileName     string         `json:"task_filename" bigquery:"task_filename"`
	Type             int64          `json:"type" bigquery:"type"`
	Anomalies        Anomalies      `json:"anomalies" bigquery:"anomalies"`
	Web100_log_entry Web100LogEntry `json:"web100_log_entry" bigquery:"web100_log_entry"`
}

func (ss *SS) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(ss)
	if err != nil {
		return bigquery.Schema{}, err
	}
	// NOTE: ideally, we would use bqx.Customize for this fix. However, the SS
	// schema uses two fields with the same name but different types (log_time
	// and web100_log_entry.log_time), and bqx.Customize would change them both.
	// This only changes the type of the necessary field.
	if len(sch) > 3 && sch[3].Name == "log_time" {
		sch[3].Type = "TIMESTAMP"
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
