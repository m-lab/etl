// This files contains schema for SideStream tests.
// Any changes here should also be made in ss.json
package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/cloud/bqx"
)

type Web100ConnectionSpecification struct {
	Local_ip           string            `json:"local_ip,string" bigquery:"local_ip"`
	Local_af           int64             `json:"local_af,int64" bigquery:"local_af"`
	Local_port         int64             `json:"local_port,int64" bigquery:"local_port"`
	Remote_ip          string            `json:"remote_ip,string" bigquery:"remote_ip"`
	Remote_port        int64             `json:"remote_port,int64" bigquery:"remote_port"`
	Local_geolocation  api.GeolocationIP `json:"local_geolocation" bigquery:"local_geolocation"`
	Remote_geolocation api.GeolocationIP `json:"remote_geolocation" bigquery:"remote_geolocation"`
}

type Web100Snap struct {
	AbruptTimeouts       int64  `json:"AbruptTimeouts,int64"`
	ActiveOpen           int64  `json:"ActiveOpen,int64"`
	CERcvd               int64  `json:"CERcvd,int64"`
	CongAvoid            int64  `json:"CongAvoid,int64"`
	CongOverCount        int64  `json:"CongOverCount,int64"`
	CongSignals          int64  `json:"CongSignals,int64"`
	CountRTT             int64  `json:"CountRTT,int64"`
	CurAppRQueue         int64  `json:"CurAppRQueue,int64"`
	CurAppWQueue         int64  `json:"CurAppWQueue,int64"`
	CurCwnd              int64  `json:"CurCwnd,int64"`
	CurMSS               int64  `json:"CurMSS,int64"`
	CurRTO               int64  `json:"CurRTO,int64"`
	CurReasmQueue        int64  `json:"CurReasmQueue,int64"`
	CurRetxQueue         int64  `json:"CurRetxQueue,int64"`
	CurRwinRcvd          int64  `json:"CurRwinRcvd,int64"`
	CurRwinSent          int64  `json:"CurRwinSent,int64"`
	CurSsthresh          int64  `json:"CurSsthresh,int64"`
	CurTimeoutCount      int64  `json:"CurTimeoutCount,int64"`
	DSACKDups            int64  `json:"DSACKDups,int64"`
	DataOctetsIn         int64  `json:"DataOctetsIn,int64"`
	DataOctetsOut        int64  `json:"DataOctetsOut,int64"`
	DataSegsIn           int64  `json:"DataSegsIn,int64"`
	DataSegsOut          int64  `json:"DataSegsOut,int64"`
	DupAckEpisodes       int64  `json:"DupAckEpisodes,int64"`
	DupAcksIn            int64  `json:"DupAcksIn,int64"`
	DupAcksOut           int64  `json:"DupAcksOut,int64"`
	Duration             int64  `json:"Duration,int64"`
	ECESent              int64  `json:"ECESent,int64"`
	ECN                  int64  `json:"ECN,int64"`
	ECNNonceRcvd         int64  `json:"ECNNonceRcvd,int64"`
	ECNsignals           int64  `json:"ECNsignals,int64"`
	ElapsedMicroSecs     int64  `json:"ElapsedMicroSecs,int64"`
	ElapsedSecs          int64  `json:"ElapsedSecs,int64"`
	FastRetran           int64  `json:"FastRetran,int64"`
	HCDataOctetsIn       int64  `json:"HCDataOctetsIn,int64"`
	HCDataOctetsOut      int64  `json:"HCDataOctetsOut,int64"`
	HCSumRTT             int64  `json:"HCSumRTT,int64"`
	HCThruOctetsAcked    int64  `json:"HCThruOctetsAcked,int64"`
	HCThruOctetsReceived int64  `json:"HCThruOctetsReceived,int64"`
	InRecovery           int64  `json:"InRecovery,int64"`
	IpTosIn              int64  `json:"IpTosIn,int64"`
	IpTosOut             int64  `json:"IpTosOut,int64"`
	IpTtl                int64  `json:"IpTtl,int64"`
	LimCwnd              int64  `json:"LimCwnd,int64"`
	LimMSS               int64  `json:"LimMSS,int64"`
	LimRwin              int64  `json:"LimRwin,int64"`
	LimSsthresh          int64  `json:"LimSsthresh,int64"`
	LocalAddress         string `json:"LocalAddress,string"`
	LocalAddressType     int64  `json:"LocalAddressType,int64"`
	LocalPort            int64  `json:"LocalPort,int64"`
	MSSRcvd              int64  `json:"MSSRcvd,int64"`
	MSSSent              int64  `json:"MSSSent,int64"`
	MaxAppRQueue         int64  `json:"MaxAppRQueue,int64"`
	MaxAppWQueue         int64  `json:"MaxAppWQueue,int64"`
	MaxCaCwnd            int64  `json:"MaxCaCwnd,int64"`
	MaxMSS               int64  `json:"MaxMSS,int64"`
	MaxPipeSize          int64  `json:"MaxPipeSize,int64"`
	MaxRTO               int64  `json:"MaxRTO,int64"`
	MaxRTT               int64  `json:"MaxRTT,int64"`
	MaxReasmQueue        int64  `json:"MaxReasmQueue,int64"`
	MaxRetxQueue         int64  `json:"MaxRetxQueue,int64"`
	MaxRwinRcvd          int64  `json:"MaxRwinRcvd,int64"`
	MaxRwinSent          int64  `json:"MaxRwinSent,int64"`
	MaxSsCwnd            int64  `json:"MaxSsCwnd,int64"`
	MaxSsthresh          int64  `json:"MaxSsthresh,int64"`
	MinMSS               int64  `json:"MinMSS,int64"`
	MinRTO               int64  `json:"MinRTO,int64"`
	MinRTT               int64  `json:"MinRTT,int64"`
	MinRwinRcvd          int64  `json:"MinRwinRcvd,int64"`
	MinRwinSent          int64  `json:"MinRwinSent,int64"`
	MinSsthresh          int64  `json:"MinSsthresh,int64"`
	Nagle                int64  `json:"Nagle,int64"`
	NonRecovDA           int64  `json:"NonRecovDA,int64"`
	NonRecovDAEpisodes   int64  `json:"NonRecovDAEpisodes,int64"`
	OctetsRetrans        int64  `json:"OctetsRetrans,int64"`
	OtherReductions      int64  `json:"OtherReductions,int64"`
	PipeSize             int64  `json:"PipeSize,int64"`
	PostCongCountRTT     int64  `json:"PostCongCountRTT,int64"`
	PostCongSumRTT       int64  `json:"PostCongSumRTT,int64"`
	PreCongSumCwnd       int64  `json:"PreCongSumCwnd,int64"`
	PreCongSumRTT        int64  `json:"PreCongSumRTT,int64"`
	QuenchRcvd           int64  `json:"QuenchRcvd,int64"`
	RTTVar               int64  `json:"RTTVar,int64"`
	RcvNxt               int64  `json:"RcvNxt,int64"`
	RcvRTT               int64  `json:"RcvRTT,int64"`
	RcvWindScale         int64  `json:"RcvWindScale,int64"`
	RecInitial           int64  `json:"RecInitial,int64"`
	RemAddress           string `json:"RemAddress,string"`
	RemPort              int64  `json:"RemPort,int64"`
	RetranThresh         int64  `json:"RetranThresh,int64"`
	SACK                 int64  `json:"SACK,int64"`
	SACKBlocksRcvd       int64  `json:"SACKBlocksRcvd,int64"`
	SACKsRcvd            int64  `json:"SACKsRcvd,int64"`
	SampleRTT            int64  `json:"SampleRTT,int64"`
	SegsIn               int64  `json:"SegsIn,int64"`
	SegsOut              int64  `json:"SegsOut,int64"`
	SegsRetrans          int64  `json:"SegsRetrans,int64"`
	SendStall            int64  `json:"SendStall,int64"`
	SlowStart            int64  `json:"SlowStart,int64"`
	SmoothedRTT          int64  `json:"SmoothedRTT,int64"`
	SndInitial           int64  `json:"SndInitial,int64"`
	SndLimBytesCwnd      int64  `json:"SndLimBytesCwnd,int64"`
	SndLimBytesRwin      int64  `json:"SndLimBytesRwin,int64"`
	SndLimBytesSender    int64  `json:"SndLimBytesSender,int64"`
	SndLimTimeCwnd       int64  `json:"SndLimTimeCwnd,int64"`
	SndLimTimeRwin       int64  `json:"SndLimTimeRwin,int64"`
	SndLimTimeSnd        int64  `json:"SndLimTimeSnd,int64"`
	SndLimTransCwnd      int64  `json:"SndLimTransCwnd,int64"`
	SndLimTransRwin      int64  `json:"SndLimTransRwin,int64"`
	SndLimTransSnd       int64  `json:"SndLimTransSnd,int64"`
	SndMax               int64  `json:"SndMax,int64"`
	SndNxt               int64  `json:"SndNxt,int64"`
	SndUna               int64  `json:"SndUna,int64"`
	SndWindScale         int64  `json:"SndWindScale,int64"`
	SoftErrorReason      int64  `json:"SoftErrorReason,int64"`
	SoftErrors           int64  `json:"SoftErrors,int64"`
	SpuriousFrDetected   int64  `json:"SpuriousFrDetected,int64"`
	SpuriousRtoDetected  int64  `json:"SpuriousRtoDetected,int64"`
	StartTimeStamp       int64  `json:"StartTimeStamp,int64"`
	State                int64  `json:"State,int64"`
	SubsequentTimeouts   int64  `json:"SubsequentTimeouts,int64"`
	SumOctetsReordered   int64  `json:"SumOctetsReordered,int64"`
	SumRTT               int64  `json:"SumRTT,int64"`
	ThruOctetsAcked      int64  `json:"ThruOctetsAcked,int64"`
	ThruOctetsReceived   int64  `json:"ThruOctetsReceived,int64"`
	TimeStamps           int64  `json:"TimeStamps,int64"`
	TimeStampRcvd        bool   `json:"TimeStampRcvd,bool"`
	TimeStampSent        bool   `json:"TimeStampSent,bool"`
	Timeouts             int64  `json:"Timeouts,int64"`
	WAD_CwndAdjust       int64  `json:"WAD_CwndAdjust,int64"`
	WAD_IFQ              int64  `json:"WAD_IFQ,int64"`
	WAD_MaxBurst         int64  `json:"WAD_MaxBurst,int64"`
	WAD_MaxSsthresh      int64  `json:"WAD_MaxSsthresh,int64"`
	WAD_NoAI             int64  `json:"WAD_NoAI,int64"`
	WillSendSACK         int64  `json:"WillSendSACK,int64"`
	WillUseSACK          int64  `json:"WillUseSACK,int64"`
	WinScaleRcvd         int64  `json:"WinScaleRcvd,int64"`
	WinScaleSent         int64  `json:"WinScaleSent,int64"`
	X_OtherReductionsCM  int64  `json:"X_OtherReductionsCM,int64"`
	X_OtherReductionsCV  int64  `json:"X_OtherReductionsCV,int64"`
	X_Rcvbuf             int64  `json:"X_Rcvbuf,int64"`
	X_Sndbuf             int64  `json:"X_Sndbuf,int64"`
	X_dbg1               int64  `json:"X_dbg1,int64"`
	X_dbg2               int64  `json:"X_dbg2,int64"`
	X_dbg3               int64  `json:"X_dbg3,int64"`
	X_dbg4               int64  `json:"X_dbg4,int64"`
	X_rcv_ssthresh       int64  `json:"X_rcv_ssthresh,int64"`
	X_wnd_clamp          int64  `json:"X_wnd_clamp,int64"`
	ZeroRwinRcvd         int64  `json:"ZeroRwinRcvd,int64"`
	ZeroRwinSent         int64  `json:"ZeroRwinSent,int64"`
}

type Web100LogEntry struct {
	LogTime         int64                         `json:"log_time" bigquery:"log_time"`
	Version         string                        `json:"version,string" bigquery:"version"`
	Group_name      string                        `json:"group_name,string" bigquery:"group_name"`
	Connection_spec Web100ConnectionSpecification `json:"connection_spec" bigquery:"connection_spec"`
	Snap            Web100Snap                    `json:"snap" bigquery:"snap"`
}

type Anomalies struct {
	Exclusion_level int64 `json:"exclusion_level" bigquery:"exclusion_level"`
}

type SS struct {
	TestID           string         `json:"test_id,string" bigquery:"test_id"`
	Project          int64          `json:"project" bigquery:"project"`
	LogTime          int64          `json:"log_time" bigquery:"log_time"`
	ParseTime        time.Time      `json:"parse_time" bigquery:"parse_time"`
	ParserVersion    string         `json:"parser_version" bigquery:"parser_version"`
	TaskFileName     string         `json:"task_filename" bigquery:"task_filename"`
	Type             int64          `json:"type" bigquery:"type"`
	Anomalies        Anomalies      `json:"anomalies" bigquery:"anomalies"`
	Web100_log_entry Web100LogEntry `json:"web100_log_entry" bigquery:"web100_log_entry"`
}

// Implement parser.Annotatable

// GetLogTime returns the timestamp that should be used for annotation.
func (ss *SS) GetLogTime() time.Time {
	// StartTimeStamp is in usec.
	return time.Unix(0, 1000*ss.Web100_log_entry.Snap.StartTimeStamp)
}

// GetClientIPs returns the client (remote) IP for annotation.  See parser.Annotatable
func (ss *SS) GetClientIPs() []string {
	return []string{ss.Web100_log_entry.Connection_spec.Remote_ip}
}

// GetServerIP returns the server (local) IP for annotation.  See parser.Annotatable
func (ss *SS) GetServerIP() string {
	return ss.Web100_log_entry.Connection_spec.Local_ip
}

// AnnotateClients adds the client annotations. See parser.Annotatable
func (ss *SS) AnnotateClients(annMap map[string]*api.Annotations) error {
	connSpec := &ss.Web100_log_entry.Connection_spec
	if annMap != nil {
		ann, ok := annMap[connSpec.Remote_ip]
		if ok && ann.Geo != nil {
			connSpec.Remote_geolocation = *ann.Geo
		}
		// TODO Handle ASN
	}
	return nil
}

// AnnotateServer adds the server annotations. See parser.Annotatable
func (ss *SS) AnnotateServer(local *api.Annotations) error {
	connSpec := &ss.Web100_log_entry.Connection_spec
	if local != nil && local.Geo != nil {
		// TODO - this should probably be a pointer
		connSpec.Local_geolocation = *local.Geo
		// TODO Handle ASN
	}
	return nil
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
	if len(sch) > 2 && sch[2].Name == "log_time" {
		sch[2].Type = "TIMESTAMP"
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
