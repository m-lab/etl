package parser_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

// TODO: IPv6 tests
func TestGetLogtime(t *testing.T) {
	fn1 := parser.PTFileName{Name: "20160112T00:45:44Z_ALL27409.paris"}
	t1, err1 := parser.GetLogtime(fn1)
	if err1 != nil || t1.String() != "2016-01-12 00:45:44 +0000 UTC" {
		t.Errorf("Error in parsing log time from legacy filename!\n")
	}

	fn2 := parser.PTFileName{Name: "20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris"}
	t2, err2 := parser.GetLogtime(fn2)
	if err2 != nil || t2.String() != "2017-03-20 23:53:10 +0000 UTC" {
		t.Errorf("Error in parsing log time from 5-tuple filename!\n")
	}

	fn3 := parser.PTFileName{Name: "20190908T000148Z_ndt-74mqr_1565960097_000000000006DBCC.jsonl"}
	t3, err3 := parser.GetLogtime(fn3)
	if err3 != nil || t3.String() != "2019-09-08 00:01:48 +0000 UTC" {
		t.Errorf("Error in parsing log time from scamper Json filename!\n")
	}
}

func TestParseJsonSimple(t *testing.T) {
	testStr := `{"UUID": "ndt-plh7v_1566050090_000000000004D64D"}
{"type":"cycle-start", "list_name":"/tmp/scamperctrl:51811", "id":1, "hostname":"ndt-plh7v", "start_time":1566691298}
{"type":"tracelb", "version":"0.1", "userid":0, "method":"icmp-echo", "src":"::ffff:180.87.97.101", "dst":"::ffff:1.47.236.62", "start":{"sec":1566691298, "usec":476221, "ftime":"2019-08-25 00:01:38"}, "probe_size":60, "firsthop":1, "attempts":3, "confidence":95, "tos":0, "gaplimit":3, "wait_timeout":5, "wait_probe":250, "probec":0, "probec_max":3000, "nodec":0, "linkc":0}
{"type":"cycle-stop", "list_name":"/tmp/scamperctrl:51811", "id":1, "hostname":"ndt-plh7v", "stop_time":1566691298}`

	output, err := parser.ParseJSON("20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.jsonl", []byte(testStr), "", "")

	log.Printf("%+v\n", output)
	if err != nil {
		t.Fatalf("Err during json parsing %v", err)
	}
	if output.UUID != "ndt-plh7v_1566050090_000000000004D64D" {
		t.Fatalf("Wrong results for UUID parsing!")
	}
	if output.Source.IP != "::ffff:180.87.97.101" || output.Destination.IP != "::ffff:1.47.236.62" {
		t.Fatalf("Wrong results for source/destination IP parsing!")
	}
	if output.ProbeSize != 60 || output.ProbeC != 0 {
		t.Fatalf("Wrong results for probe size or probec parsing!")
	}
}

func TestParseJsonComplex2(t *testing.T) {
	testStr := `
	{"UUID": "ndt-x7jcx_1559749627_0000000000008E7B"}
{"type":"cycle-start", "list_name":"/tmp/scamperctrl:5394", "id":1, "hostname":"ndt-x7jcx", "start_time":1559894119}
{"type":"tracelb", "version":"0.1", "userid":0, "method":"icmp-echo", "src":"2001:5a0:3a01::101", "dst":"2600:1700:d270:5ab0:95d9:83d7:f088:29ca", "start":{"sec":1559894337, "usec":478335, "ftime":"2019-06-07 07:58:57"}, "probe_size":60, "firsthop":1, "attempts":3, "confidence":95, "tos":0, "gaplimit":3, "wait_timeout":5, "wait_probe":250, "probec":103, "probec_max":3000, "nodec":6, "linkc":6, "nodes":[{"addr":"2001:5a0:3a01::1", "name":"ix-xe-2-2-4-0.tcore1.eql-los-angeles.ipv6.as6453.net", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:5a0:fff0:100::55", "probes":[{"tx":{"sec":1559894337, "usec":729105}, "replyc":1, "ttl":2, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1559894337, "usec":729390}, "ttl":63, "rtt":0.285, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894337, "usec":979851}, "replyc":1, "ttl":2, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1559894337, "usec":980160}, "ttl":63, "rtt":0.309, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894338, "usec":230606}, "replyc":1, "ttl":2, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1559894338, "usec":230911}, "ttl":63, "rtt":0.305, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894338, "usec":481320}, "replyc":1, "ttl":2, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1559894338, "usec":481593}, "ttl":63, "rtt":0.273, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894338, "usec":732692}, "replyc":1, "ttl":2, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1559894338, "usec":749793}, "ttl":63, "rtt":17.101, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894338, "usec":983200}, "replyc":1, "ttl":2, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1559894338, "usec":983479}, "ttl":63, "rtt":0.279, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:5a0:fff0:100::55", "name":"if-ae-6-20.tcore2.lvw-los-angeles.ipv6.as6453.net", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:5a0:fff0::1", "probes":[{"tx":{"sec":1559894339, "usec":233746}, "replyc":1, "ttl":3, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1559894339, "usec":234056}, "ttl":63, "rtt":0.310, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894339, "usec":484606}, "replyc":1, "ttl":3, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1559894339, "usec":484913}, "ttl":63, "rtt":0.307, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894339, "usec":735355}, "replyc":1, "ttl":3, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1559894339, "usec":735687}, "ttl":63, "rtt":0.332, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894339, "usec":986091}, "replyc":1, "ttl":3, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1559894339, "usec":986372}, "ttl":63, "rtt":0.281, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894340, "usec":236711}, "replyc":1, "ttl":3, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1559894340, "usec":236992}, "ttl":63, "rtt":0.281, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894340, "usec":487374}, "replyc":1, "ttl":3, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1559894340, "usec":487683}, "ttl":63, "rtt":0.309, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:5a0:fff0::1", "name":"if-ae-2-2.tcore1.lvw-los-angeles.ipv6.as6453.net", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:5a0:2e00:500::6", "probes":[{"tx":{"sec":1559894340, "usec":737884}, "replyc":1, "ttl":4, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1559894340, "usec":740908}, "ttl":62, "rtt":3.024, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894340, "usec":988310}, "replyc":1, "ttl":4, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1559894340, "usec":992824}, "ttl":62, "rtt":4.514, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894341, "usec":239213}, "replyc":1, "ttl":4, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1559894341, "usec":240841}, "ttl":62, "rtt":1.628, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894341, "usec":490287}, "replyc":1, "ttl":4, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1559894341, "usec":492741}, "ttl":62, "rtt":2.454, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894341, "usec":741147}, "replyc":1, "ttl":4, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1559894341, "usec":744687}, "ttl":62, "rtt":3.540, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894341, "usec":992178}, "replyc":1, "ttl":4, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1559894341, "usec":996793}, "ttl":62, "rtt":4.615, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:5a0:2e00:500::6", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:1890:ff:ffff:12:122:128:98", "probes":[{"tx":{"sec":1559894342, "usec":242640}, "replyc":1, "ttl":5, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1559894342, "usec":246062}, "ttl":60, "rtt":3.422, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894342, "usec":493524}, "replyc":1, "ttl":5, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1559894342, "usec":502088}, "ttl":60, "rtt":8.564, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894342, "usec":743900}, "replyc":1, "ttl":5, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1559894342, "usec":750000}, "ttl":60, "rtt":6.100, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894342, "usec":994419}, "replyc":1, "ttl":5, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1559894342, "usec":997918}, "ttl":60, "rtt":3.499, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894343, "usec":245348}, "replyc":1, "ttl":5, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1559894343, "usec":254066}, "ttl":60, "rtt":8.718, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1559894343, "usec":495413}, "replyc":1, "ttl":5, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1559894343, "usec":502837}, "ttl":60, "rtt":7.424, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:1890:ff:ffff:12:122:128:98", "name":"la2ca21crs.ipv6.att.net", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:1890:ff:ffff:12:123:136:189", "probes":[{"tx":{"sec":1559894343, "usec":746050}, "replyc":1, "ttl":6, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1559894343, "usec":746757}, "ttl":61, "rtt":0.707, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":2}]},{"tx":{"sec":1559894343, "usec":996152}, "replyc":1, "ttl":6, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1559894343, "usec":996831}, "ttl":61, "rtt":0.679, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":2}]},{"tx":{"sec":1559894344, "usec":246165}, "replyc":1, "ttl":6, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1559894344, "usec":246840}, "ttl":61, "rtt":0.675, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":2}]},{"tx":{"sec":1559894344, "usec":496454}, "replyc":1, "ttl":6, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1559894344, "usec":497175}, "ttl":61, "rtt":0.721, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":2}]},{"tx":{"sec":1559894344, "usec":746519}, "replyc":1, "ttl":6, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1559894344, "usec":747228}, "ttl":61, "rtt":0.709, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":2}]},{"tx":{"sec":1559894344, "usec":997540}, "replyc":1, "ttl":6, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1559894344, "usec":998242}, "ttl":61, "rtt":0.702, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":2}]}]}]]},{"addr":"2001:1890:ff:ffff:12:123:136:189", "q_ttl":2, "linkc":1, "links":[[{"addr":"2001:506:6000:131:75:29:10:206", "probes":[{"tx":{"sec":1559894345, "usec":248273}, "replyc":1, "ttl":7, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1559894377, "usec":200272}, "ttl":60, "rtt":31951.999, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894350, "usec":248306}, "replyc":1, "ttl":7, "attempt":1, "flowid":1, "replies":[{"rx":{"sec":1559894382, "usec":200208}, "ttl":60, "rtt":31951.902, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894355, "usec":248382}, "replyc":1, "ttl":7, "attempt":2, "flowid":1, "replies":[{"rx":{"sec":1559894387, "usec":199573}, "ttl":60, "rtt":31951.191, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894360, "usec":248939}, "replyc":1, "ttl":7, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1559894392, "usec":99250}, "ttl":60, "rtt":31850.311, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894375, "usec":253239}, "replyc":1, "ttl":7, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1559894408, "usec":402136}, "ttl":60, "rtt":33148.897, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894380, "usec":254308}, "replyc":1, "ttl":7, "attempt":1, "flowid":3, "replies":[{"rx":{"sec":1559894413, "usec":370240}, "ttl":60, "rtt":33115.932, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894390, "usec":257130}, "replyc":1, "ttl":7, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1559894423, "usec":599616}, "ttl":60, "rtt":33342.486, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894395, "usec":258137}, "replyc":1, "ttl":7, "attempt":1, "flowid":4, "replies":[{"rx":{"sec":1559894428, "usec":738937}, "ttl":60, "rtt":33480.800, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]{"tx":{"sec":1559894400, "usec":260078}, "replyc":1, "ttl":7, "attempt":2, "flowid":4, "replies":[{"rx":{"sec":1559894433, "usec":658654}, "ttl":60, "rtt":33398.576, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}, {"addr":"*"}],[{"addr":"*"}],[{"addr":"*"}],]}]}
{"type":"cycle-stop", "list_name":"/tmp/scamperctrl:5394", "id":1, "hostname":"ndt-x7jcx", "stop_time":1559894705}`

	_, err := parser.ParseJSON("20190607T000138Z_ndt-plh7v_1566050090_000000000004D64D.jsonl", []byte(testStr), "", "")

	t.Fatalf("Err %v", err)
}

func TestParseJsonComplex(t *testing.T) {
	testStr := `{"UUID": "ndt-plh7v_1566050090_000000000004D60F"}
	{"type":"cycle-start", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "start_time":1566691268}
	{"type":"tracelb", "version":"0.1", "userid":0, "method":"icmp-echo", "src":"2001:550:1b01:1:e41d:2d00:151:f6c0", "dst":"2600:1009:b013:1a59:c369:b528:98fd:ab43", "start":{"sec":1567900908, "usec":729543, "ftime":"2019-09-08 00:01:48"}, "probe_size":60, "firsthop":1, "attempts":3, "confidence":95, "tos":0, "gaplimit":3, "wait_timeout":5, "wait_probe":250, "probec":85, "probec_max":3000, "nodec":6, "linkc":6, "nodes":[{"addr":"2001:550:1b01:1::1", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:550:3::1ca", "probes":[{"tx":{"sec":1567900908, "usec":979595}, "replyc":1, "ttl":2, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900909, "usec":16398}, "ttl":63, "rtt":36.803, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":229642}, "replyc":1, "ttl":2, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900909, "usec":229974}, "ttl":63, "rtt":0.332, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":480242}, "replyc":1, "ttl":2, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900909, "usec":480571}, "ttl":63, "rtt":0.329, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":730987}, "replyc":1, "ttl":2, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900909, "usec":731554}, "ttl":63, "rtt":0.567, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":982029}, "replyc":1, "ttl":2, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900909, "usec":982358}, "ttl":63, "rtt":0.329, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":232994}, "replyc":1, "ttl":2, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900910, "usec":234231}, "ttl":63, "rtt":1.237, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:550:3::1ca", "q_ttl":1, "linkc":1, "links":[[{"addr":"2600:803::79", "probes":[{"tx":{"sec":1567900910, "usec":483606}, "replyc":1, "ttl":3, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900910, "usec":500939}, "ttl":58, "rtt":17.333, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":734394}, "replyc":1, "ttl":3, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900910, "usec":752612}, "ttl":58, "rtt":18.218, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":985425}, "replyc":1, "ttl":3, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900911, "usec":6498}, "ttl":58, "rtt":21.073, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":235481}, "replyc":1, "ttl":3, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900911, "usec":252800}, "ttl":58, "rtt":17.319, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":486164}, "replyc":1, "ttl":3, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900911, "usec":503522}, "ttl":58, "rtt":17.358, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":737096}, "replyc":1, "ttl":3, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900911, "usec":760439}, "ttl":58, "rtt":23.343, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2600:803::79", "q_ttl":1, "linkc":1, "links":[[{"addr":"2600:803:150f::4a", "probes":[{"tx":{"sec":1567900911, "usec":987801}, "replyc":1, "ttl":4, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900912, "usec":10282}, "ttl":57, "rtt":22.481, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":238227}, "replyc":1, "ttl":4, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900912, "usec":262270}, "ttl":57, "rtt":24.043, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":539699}, "replyc":1, "ttl":4, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900912, "usec":562078}, "ttl":57, "rtt":22.379, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":789753}, "replyc":1, "ttl":4, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900912, "usec":812145}, "ttl":57, "rtt":22.392, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":42261}, "replyc":1, "ttl":4, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900913, "usec":64678}, "ttl":57, "rtt":22.417, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":292682}, "replyc":1, "ttl":4, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900913, "usec":315254}, "ttl":57, "rtt":22.572, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2600:803:150f::4a", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:4888:36:1002:3a2:1:0:1", "probes":[{"tx":{"sec":1567900913, "usec":543335}, "replyc":1, "ttl":5, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900913, "usec":568980}, "ttl":56, "rtt":25.645, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":793793}, "replyc":1, "ttl":5, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900913, "usec":816848}, "ttl":56, "rtt":23.055, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":43821}, "replyc":1, "ttl":5, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900914, "usec":72827}, "ttl":56, "rtt":29.006, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":294820}, "replyc":1, "ttl":5, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900914, "usec":320815}, "ttl":56, "rtt":25.995, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":545802}, "replyc":1, "ttl":5, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900914, "usec":568924}, "ttl":56, "rtt":23.122, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":796839}, "replyc":1, "ttl":5, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900914, "usec":824735}, "ttl":56, "rtt":27.896, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:4888:36:1002:3a2:1:0:1", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:4888:3f:6092:3a2:26:0:1", "probes":[{"tx":{"sec":1567900915, "usec":46897}, "replyc":1, "ttl":6, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900915, "usec":69996}, "ttl":245, "rtt":23.099, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":297455}, "replyc":1, "ttl":6, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900915, "usec":320524}, "ttl":245, "rtt":23.069, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":547737}, "replyc":1, "ttl":6, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900915, "usec":570899}, "ttl":245, "rtt":23.162, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":798167}, "replyc":1, "ttl":6, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900915, "usec":821218}, "ttl":245, "rtt":23.051, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900916, "usec":55367}, "replyc":1, "ttl":6, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900916, "usec":78485}, "ttl":245, "rtt":23.118, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900916, "usec":306410}, "replyc":1, "ttl":6, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900916, "usec":329419}, "ttl":245, "rtt":23.009, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:4888:3f:6092:3a2:26:0:1", "q_ttl":1, "linkc":1, "links":[[{"addr":"*"}],[{"addr":"*"}],]}]}
	{"type":"cycle-stop", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "stop_time":1566691541}`
	output, err := parser.ParseJSON("20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.jsonl", []byte(testStr), "", "")

	expectedHop := schema.ScamperHop{
		Source: schema.HopIP{IP: "2001:550:1b01:1::1", ASN: 0},
		Linkc:  1,
		Links: []schema.HopLink{
			schema.HopLink{
				HopDstIP: "2001:550:3::1ca",
				TTL:      2,
				Probes: []schema.HopProbe{
					schema.HopProbe{Flowid: 1, Rtt: []float64{36.803}},
					schema.HopProbe{Flowid: 2, Rtt: []float64{0.332}},
					schema.HopProbe{Flowid: 3, Rtt: []float64{0.329}},
					schema.HopProbe{Flowid: 4, Rtt: []float64{0.567}},
					schema.HopProbe{Flowid: 5, Rtt: []float64{0.329}},
					schema.HopProbe{Flowid: 6, Rtt: []float64{1.237}},
				},
			},
		},
	}

	if err != nil {
		t.Fatalf("Err during json parsing %v", err)
	}

	if output.UUID != "ndt-plh7v_1566050090_000000000004D60F" {
		t.Fatalf("Wrong results for UUID parsing!")
	}
	if output.Source.IP != "2001:550:1b01:1:e41d:2d00:151:f6c0" || output.Destination.IP != "2600:1009:b013:1a59:c369:b528:98fd:ab43" {
		t.Fatalf("Wrong results for source/destination IP parsing!")
	}
	if output.ProbeSize != 60 || output.ProbeC != 85 {
		t.Fatalf("Wrong results for probe size or probec parsing!")
	}
	if !reflect.DeepEqual(output.Hop[0], expectedHop) {
		fmt.Printf("Here is expected    : %+v\n", expectedHop)
		fmt.Printf("Here is what is real: %+v\n", output.Hop[0])
		t.Fatalf("Wrong results for Json hops parsing!")
	}
}

func TestParseFirstLine(t *testing.T) {
	protocol, dest_ip, server_ip, err := parser.ParseFirstLine("traceroute [(64.86.132.76:33461) -> (98.162.212.214:53849)], protocol icmp, algo exhaustive, duration 19 s")
	if dest_ip != "98.162.212.214" || server_ip != "64.86.132.76" || protocol != "icmp" || err != nil {
		t.Errorf("Error in parsing the first line!\n")
		return
	}

	protocol, dest_ip, server_ip, err = parser.ParseFirstLine("Exception : [ERROR](Probe.cc, 109)Can't send the probe : Invalid argument")
	if err == nil {
		t.Errorf("Error in parsing the first line!\n")
		return
	}

}

func TestCreateTestId(t *testing.T) {
	test_id := parser.CreateTestId("20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz", "20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris")
	if test_id != "2017/05/01/mlab1.acc02/20170501T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris.gz" {
		fmt.Println(test_id)
		t.Errorf("Error in creating test id!\n")
		return
	}
}

func TestParseLegacyFormatData(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20160112T00:45:44Z_ALL27409.paris")
	if err != nil {
		fmt.Println("cannot load test data")
		return
	}
	cachedTest, err := parser.Parse(nil, "testdata/20160112T00:45:44Z_ALL27409.paris", "", rawData, "pt-daily")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(cachedTest.Hops) != 9 {
		t.Fatalf("Do not process hops correctly.")
	}
	if cachedTest.LogTime.Unix() != 1452559544 {
		t.Fatalf("Do not process log time correctly.")
	}
	if cachedTest.LastValidHopLine != "ExpectedDestIP" {
		t.Fatalf("Did not reach expected destination.")
	}
}

func TestPTParser(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris")
	cachedTest, err := parser.Parse(nil, "testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris", "", rawData, "pt-daily")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if cachedTest.LogTime.Unix() != 1490053990 {
		t.Fatalf("Do not process log time correctly.")
	}

	if cachedTest.Source.IP != "172.17.94.34" {
		t.Fatalf("Wrong results for Server IP.")
	}

	if cachedTest.Destination.IP != "74.125.224.100" {
		t.Fatalf("Wrong results for Client IP.")
	}

	// TODO(dev): reformat these individual values to be more readable.
	expected_hop := schema.ScamperHop{
		Source: schema.HopIP{
			IP:          "64.233.174.109",
			City:        "",
			CountryCode: "",
			Hostname:    "sr05-te1-8.nuq04.net.google.com",
		},
		Linkc: 0,
		Links: []schema.HopLink{
			schema.HopLink{
				HopDstIP: "74.125.224.100",
				TTL:      0,
				Probes: []schema.HopProbe{
					schema.HopProbe{
						Flowid: 0,
						Rtt:    []float64{0.895},
					},
				},
			},
		},
	}
	if len(cachedTest.Hops) != 38 {
		t.Fatalf("Wrong number of PT hops!")
	}

	if !reflect.DeepEqual(cachedTest.Hops[0], expected_hop) {
		fmt.Printf("Here is expected    : %v\n", expected_hop)
		fmt.Printf("Here is what is real: %v\n", cachedTest.Hops[0])
		t.Fatalf("Wrong results for PT hops!")
	}
}

func TestPTInserter(t *testing.T) {
	ins := newInMemoryInserter()
	pt := parser.NewPTParser(ins)
	rawData, err := ioutil.ReadFile("testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris")
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}
	meta := map[string]bigquery.Value{"filename": "gs://fake-bucket/fake-archive.tgz"}
	err = pt.ParseAndInsert(meta, "testdata/20170320T23:53:10Z-172.17.94.34-33456-74.125.224.100-33457.paris", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if pt.NumRowsForTest() != 1 {
		fmt.Println(pt.NumRowsForTest())
		t.Fatalf("Number of rows in PT table is wrong.")
	}
	pt.AnnotateAndPutAsync("traceroute")
	//pt.Inserter.Flush()
	if len(ins.data) != 1 {
		fmt.Println(len(ins.data))
		t.Fatalf("Number of rows in inserter is wrong.")
	}
	if ins.data[0].(*schema.PTTest).Parseinfo.TaskFileName != "gs://fake-bucket/fake-archive.tgz" {
		t.Fatalf("Task filename is wrong.")
	}
}

func TestPTPollutionCheck(t *testing.T) {
	ins := &inMemoryInserter{}
	pt := parser.NewPTParser(ins)

	tests := []struct {
		fileName             string
		expectedBufferedTest int
		expectedNumRows      int
	}{
		{
			fileName:             "testdata/PT/20171208T00:00:04Z-35.188.101.1-40784-173.205.3.38-9090.paris",
			expectedBufferedTest: 1,
			expectedNumRows:      0,
		},
		{
			fileName: "testdata/PT/20171208T00:00:04Z-37.220.21.130-5667-173.205.3.43-42487.paris",
			// The second test reached expected destIP, and was inserted into BigQuery table.
			// The buffer has only the first test.
			expectedBufferedTest: 1,
			expectedNumRows:      1,
		},
		{
			fileName: "testdata/PT/20171208T00:00:14Z-139.60.160.135-2023-173.205.3.44-1101.paris",
			// The first test was detected that it was polluted by the third test.
			// expectedBufferedTest is 0, which means pollution detected and test removed.
			expectedBufferedTest: 0,
			// The third test reached its destIP and was inserted into BigQuery.
			expectedNumRows: 2,
		},
		{
			fileName: "testdata/PT/20171208T00:00:14Z-76.227.226.149-37156-173.205.3.37-52156.paris",
			// The 4th test was buffered.
			expectedBufferedTest: 1,
			expectedNumRows:      2,
		},
		{
			fileName: "testdata/PT/20171208T22:03:54Z-104.198.139.160-60574-163.22.28.37-7999.paris",
			// The 5th test was buffered too.
			expectedBufferedTest: 2,
			expectedNumRows:      2,
		},
		{
			fileName: "testdata/PT/20171208T22:03:59Z-139.60.160.135-1519-163.22.28.44-1101.paris",
			// The 5th test was detected that was polluted by the 6th test.
			// It was removed from buffer (expectedBufferedTest drop from 2 to 1).
			// Buffer contains the 4th test now.
			expectedBufferedTest: 1,
			// The 6th test reached its destIP and was inserted into BigQuery.
			expectedNumRows: 3,
		},
	}

	// Process the tests
	for _, test := range tests {
		rawData, err := ioutil.ReadFile(test.fileName)
		if err != nil {
			t.Fatalf("cannot read testdata.")
		}
		meta := map[string]bigquery.Value{"filename": test.fileName, "parse_time": time.Now()}
		err = pt.ParseAndInsert(meta, test.fileName, rawData)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if pt.NumBufferedTests() != test.expectedBufferedTest {
			t.Fatalf("Data not buffered correctly")
		}
		if pt.NumRowsForTest() != test.expectedNumRows {
			t.Fatalf("Data of test %s not inserted into BigQuery correctly. Expect %d Actually %d", test.fileName, test.expectedNumRows, ins.RowsInBuffer())
		}
	}

	// Insert the 4th test in the buffer to BigQuery.
	pt.ProcessLastTests()
	if pt.NumRowsForTest() != 4 {
		t.Fatalf("Number of tests in buffer not correct, expect 4, actually %d.", ins.RowsInBuffer())
	}
}

func TestPTEmptyTest(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris")
	if err != nil {
		fmt.Println("cannot load test data")
		return
	}
	_, parseErr := parser.Parse(nil, "testdata/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris", "", rawData, "pt-daily")
	if parseErr != nil {
		t.Fatal(parseErr)
	}
}
