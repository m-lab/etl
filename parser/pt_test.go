package parser_test

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

func TestParseTraceroute(t *testing.T) {
	testStr := `{"uuid":"\"ndt-plh7v_1566050090_000000000004D64D\"","testtime":"0001-01-01T00:00:00Z","parseinfo":{"TaskFileName":"","ParseTime":"0001-01-01T00:00:00Z","ParserVersion":"","Filename":""},"start_time":1566691298,"stop_time":1566691298,"scamper_version":"\"0.1\"","source":{"IP":"::ffff:180.87.97.101","Port":0,"IATA":"","Geo":null,"Network":null},"destination":{"IP":"::ffff:1.47.236.62","Port":0,"Geo":null,"Network":null},"probe_size":60,"probec":0,"hop":null,"exp_version":"\"\"","cached_result":false}`
	_, err := parser.ParsePT("20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.json", []byte(testStr), "", "")
	if err != nil {
		t.Fatalf("Err during json parsing %v", err)
	}

	testStr2 := `{"uuid":"\"ndt-rfwqf_1588021292_0000000000000242\"","testtime":"0001-01-01T00:00:00Z","parseinfo":{"TaskFileName":"","ParseTime":"0001-01-01T00:00:00Z","ParserVersion":"","Filename":""},"start_time":1588021658,"stop_time":1588021755,"scamper_version":"\"0.1\"","source":{"IP":"2001:1900:2100:2d::75","Port":0,"IATA":"","Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822,"radius":100},"Network":{"Systems":[{"ASNs":[3356]}]}},"destination":{"IP":"2600:3c02::17:d803","Port":0,"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822,"radius":100},"Network":{"Systems":[{"ASNs":[63949]}]}},"probe_size":60,"probec":68,"hop":[{"source":{"ip":"\"2001:1900:2100:2d::1\"","city":"\"\"","country_code":"\"US\"","hostname":"\"8-2-40.ear2.NewYork1.Level3.net\"","asn":3356},"linkc":1,"link":[{"hop_dst_ip":"\"2001:1900::3:1c0\"","ttl":2,"probes":[{"flowid":1,"rtt":[0.403]},{"flowid":2,"rtt":[0.404]},{"flowid":3,"rtt":[0.374]},{"flowid":4,"rtt":[0.411]},{"flowid":5,"rtt":[0.396]},{"flowid":6,"rtt":[0.418]}]}]},{"source":{"ip":"\"2001:1900:2100:2d::1\"","city":"\"\"","country_code":"\"\"","hostname":"\"8-2-40.ear2.NewYork1.Level3.net\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2001:1900::3:1c0\"","ttl":2,"probes":[{"flowid":1,"rtt":[0.403]},{"flowid":2,"rtt":[0.404]},{"flowid":3,"rtt":[0.374]},{"flowid":4,"rtt":[0.411]},{"flowid":5,"rtt":[0.396]},{"flowid":6,"rtt":[0.418]}]}]},{"source":{"ip":"\"2001:1900::3:1c0\"","city":"\"\"","country_code":"\"US\"","hostname":"\"lo-0-v6.ear2.NewYork2.Level3.net\"","asn":3356},"linkc":1,"link":[{"hop_dst_ip":"\"2001:1900:4:3::246\"","ttl":3,"probes":[{"flowid":1,"rtt":[5.215]},{"flowid":2,"rtt":[6.718]},{"flowid":3,"rtt":[3.359]},{"flowid":4,"rtt":[2.588]},{"flowid":5,"rtt":[8.552]},{"flowid":6,"rtt":[7.959]}]}]},{"source":{"ip":"\"2001:1900::3:1c0\"","city":"\"\"","country_code":"\"\"","hostname":"\"lo-0-v6.ear2.NewYork2.Level3.net\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2001:1900:4:3::246\"","ttl":3,"probes":[{"flowid":1,"rtt":[5.215]},{"flowid":2,"rtt":[6.718]},{"flowid":3,"rtt":[3.359]},{"flowid":4,"rtt":[2.588]},{"flowid":5,"rtt":[8.552]},{"flowid":6,"rtt":[7.959]}]}]},{"source":{"ip":"\"2001:1900:4:3::246\"","city":"\"\"","country_code":"\"US\"","hostname":"\"Tata-level3-2x10G.NewYork.Level3.net\"","asn":3356},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:12:200::11\"","ttl":4,"probes":[{"flowid":1,"rtt":[3.962]},{"flowid":2,"rtt":[0.602]},{"flowid":3,"rtt":[1.437]},{"flowid":4,"rtt":[4.275]},{"flowid":5,"rtt":null},{"flowid":5,"rtt":[0.993]},{"flowid":6,"rtt":[3.422]}]}]},{"source":{"ip":"\"2001:1900:4:3::246\"","city":"\"\"","country_code":"\"\"","hostname":"\"Tata-level3-2x10G.NewYork.Level3.net\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:12:200::11\"","ttl":4,"probes":[{"flowid":1,"rtt":[3.962]},{"flowid":2,"rtt":[0.602]},{"flowid":3,"rtt":[1.437]},{"flowid":4,"rtt":[4.275]},{"flowid":5,"rtt":null},{"flowid":5,"rtt":[0.993]},{"flowid":6,"rtt":[3.422]}]}]},{"source":{"ip":"\"2001:5a0:12:200::11\"","city":"\"\"","country_code":"\"US\"","hostname":"\"if-ae-12-2.tcore2.nto-new-york.ipv6.as6453.net\"","asn":6453},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:a00::31\"","ttl":5,"probes":[{"flowid":1,"rtt":[12.486]},{"flowid":2,"rtt":[12.984]},{"flowid":3,"rtt":[12.993]},{"flowid":4,"rtt":[6.794]},{"flowid":5,"rtt":[13.277]},{"flowid":6,"rtt":[14.314]}]}]},{"source":{"ip":"\"2001:5a0:12:200::11\"","city":"\"\"","country_code":"\"\"","hostname":"\"if-ae-12-2.tcore2.nto-new-york.ipv6.as6453.net\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:a00::31\"","ttl":5,"probes":[{"flowid":1,"rtt":[12.486]},{"flowid":2,"rtt":[12.984]},{"flowid":3,"rtt":[12.993]},{"flowid":4,"rtt":[6.794]},{"flowid":5,"rtt":[13.277]},{"flowid":6,"rtt":[14.314]}]}]},{"source":{"ip":"\"2001:5a0:a00::31\"","city":"\"\"","country_code":"\"US\"","hostname":"\"if-ae-30-2.tcore1.aeq-ashburn.ipv6.as6453.net\"","asn":6453},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:600:400::31\"","ttl":6,"probes":[{"flowid":1,"rtt":[20.958]},{"flowid":2,"rtt":[22.261]},{"flowid":3,"rtt":[20.446]},{"flowid":4,"rtt":[21.531]},{"flowid":5,"rtt":[25.063]},{"flowid":6,"rtt":[21.826]}]}]},{"source":{"ip":"\"2001:5a0:a00::31\"","city":"\"\"","country_code":"\"\"","hostname":"\"if-ae-30-2.tcore1.aeq-ashburn.ipv6.as6453.net\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:600:400::31\"","ttl":6,"probes":[{"flowid":1,"rtt":[20.958]},{"flowid":2,"rtt":[22.261]},{"flowid":3,"rtt":[20.446]},{"flowid":4,"rtt":[21.531]},{"flowid":5,"rtt":[25.063]},{"flowid":6,"rtt":[21.826]}]}]},{"source":{"ip":"\"2001:5a0:600:400::31\"","city":"\"\"","country_code":"\"US\"","hostname":"\"if-ae-21-2.tcore2.a56-atlanta.ipv6.as6453.net\"","asn":6453},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:a00:500::91\"","ttl":7,"probes":[{"flowid":1,"rtt":[21.138]},{"flowid":2,"rtt":[22.341]},{"flowid":3,"rtt":[23.403]},{"flowid":4,"rtt":[23.443]},{"flowid":5,"rtt":[17.639]},{"flowid":6,"rtt":[21.299]}]}]},{"source":{"ip":"\"2001:5a0:600:400::31\"","city":"\"\"","country_code":"\"\"","hostname":"\"if-ae-21-2.tcore2.a56-atlanta.ipv6.as6453.net\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2001:5a0:a00:500::91\"","ttl":7,"probes":[{"flowid":1,"rtt":[21.138]},{"flowid":2,"rtt":[22.341]},{"flowid":3,"rtt":[23.403]},{"flowid":4,"rtt":[23.443]},{"flowid":5,"rtt":[17.639]},{"flowid":6,"rtt":[21.299]}]}]},{"source":{"ip":"\"2600:3c02:4444:7::2\"","city":"\"\"","country_code":"\"US\"","hostname":"\"\"","asn":63949},"linkc":1,"link":[{"hop_dst_ip":"\"2600:3c02::17:d001\"","ttl":10,"probes":[{"flowid":1,"rtt":[22.562]},{"flowid":2,"rtt":[26.205]},{"flowid":3,"rtt":[23.137]},{"flowid":4,"rtt":[25.452]},{"flowid":5,"rtt":[25.448]},{"flowid":6,"rtt":[21.853]}]}]},{"source":{"ip":"\"2600:3c02:4444:7::2\"","city":"\"\"","country_code":"\"\"","hostname":"\"\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2600:3c02::17:d001\"","ttl":10,"probes":[{"flowid":1,"rtt":[22.562]},{"flowid":2,"rtt":[26.205]},{"flowid":3,"rtt":[23.137]},{"flowid":4,"rtt":[25.452]},{"flowid":5,"rtt":[25.448]},{"flowid":6,"rtt":[21.853]}]}]},{"source":{"ip":"\"2600:3c02::17:d001\"","city":"\"\"","country_code":"\"US\"","hostname":"\"\"","asn":63949},"linkc":1,"link":[{"hop_dst_ip":"\"2600:3c02::17:d803\"","ttl":11,"probes":[{"flowid":1,"rtt":[27.971]}]}]},{"source":{"ip":"\"2600:3c02::17:d001\"","city":"\"\"","country_code":"\"\"","hostname":"\"\"","asn":0},"linkc":1,"link":[{"hop_dst_ip":"\"2600:3c02::17:d803\"","ttl":11,"probes":[{"flowid":1,"rtt":[27.971]}]}]}],"exp_version":"\"d970fab\"","cached_result":true}`
	output, err := parser.ParsePT("20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.json", []byte(testStr2), "", "fake.tgz")
	if err != nil {
		t.Fatalf("Err during json parsing %v", err)
	}
	if output.UUID != "ndt-rfwqf_1588021292_0000000000000242" {
		t.Fatalf("Err to get correct UUID")
	}
	if !output.TestTime.Equal(time.Date(2019, 8, 25, 0, 1, 38, 0, time.UTC)) {
		t.Fatalf("Err to get correct test time")
	}
	if output.Parseinfo.Filename != "20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.json" {
		t.Fatalf("Err to get correct filename")
	}
}

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
{"type":"cycle-stop", "list_name":"/tmp/scamperctrl:51811", "id":1, "hostname":"ndt-plh7v", "stop_time":1566691298}
`

	output, err := parser.ParseJSON("20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.jsonl", []byte(testStr), "", "")

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
	if output.Parseinfo.Filename != "20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.jsonl" {
		t.Fatalf("Wrong results for filename parsing!")
	}
}

func TestParseJsonNoLinks(t *testing.T) {
	// Last object on the "type":"tracelb" line has "linkc":1 but no "links" set.
	testStr := `{"UUID": "ndt-plh7v_1566050090_000000000004D60F"}
	{"type":"cycle-start", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "start_time":1566691268}
	{"type":"tracelb", "version":"0.1", "userid":0, "method":"icmp-echo", "src":"2001:550:1b01:1:e41d:2d00:151:f6c0", "dst":"2600:1009:b013:1a59:c369:b528:98fd:ab43", "start":{"sec":1567900908, "usec":729543, "ftime":"2019-09-08 00:01:48"}, "probe_size":60, "firsthop":1, "attempts":3, "confidence":95, "tos":0, "gaplimit":3, "wait_timeout":5, "wait_probe":250, "probec":85, "probec_max":3000, "nodec":6, "linkc":6, "nodes":[{"addr":"2001:550:1b01:1::1", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:550:3::1ca", "probes":[{"tx":{"sec":1567900908, "usec":979595}, "replyc":1, "ttl":2, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900909, "usec":16398}, "ttl":63, "rtt":36.803, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":229642}, "replyc":1, "ttl":2, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900909, "usec":229974}, "ttl":63, "rtt":0.332, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":480242}, "replyc":1, "ttl":2, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900909, "usec":480571}, "ttl":63, "rtt":0.329, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":730987}, "replyc":1, "ttl":2, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900909, "usec":731554}, "ttl":63, "rtt":0.567, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":982029}, "replyc":1, "ttl":2, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900909, "usec":982358}, "ttl":63, "rtt":0.329, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":232994}, "replyc":1, "ttl":2, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900910, "usec":234231}, "ttl":63, "rtt":1.237, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:550:3::1ca", "q_ttl":1, "linkc":1, "links":[[{"addr":"2600:803::79", "probes":[{"tx":{"sec":1567900910, "usec":483606}, "replyc":1, "ttl":3, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900910, "usec":500939}, "ttl":58, "rtt":17.333, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":734394}, "replyc":1, "ttl":3, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900910, "usec":752612}, "ttl":58, "rtt":18.218, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":985425}, "replyc":1, "ttl":3, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900911, "usec":6498}, "ttl":58, "rtt":21.073, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":235481}, "replyc":1, "ttl":3, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900911, "usec":252800}, "ttl":58, "rtt":17.319, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":486164}, "replyc":1, "ttl":3, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900911, "usec":503522}, "ttl":58, "rtt":17.358, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":737096}, "replyc":1, "ttl":3, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900911, "usec":760439}, "ttl":58, "rtt":23.343, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2600:803::79", "q_ttl":1, "linkc":1, "links":[[{"addr":"2600:803:150f::4a", "probes":[{"tx":{"sec":1567900911, "usec":987801}, "replyc":1, "ttl":4, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900912, "usec":10282}, "ttl":57, "rtt":22.481, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":238227}, "replyc":1, "ttl":4, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900912, "usec":262270}, "ttl":57, "rtt":24.043, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":539699}, "replyc":1, "ttl":4, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900912, "usec":562078}, "ttl":57, "rtt":22.379, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":789753}, "replyc":1, "ttl":4, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900912, "usec":812145}, "ttl":57, "rtt":22.392, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":42261}, "replyc":1, "ttl":4, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900913, "usec":64678}, "ttl":57, "rtt":22.417, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":292682}, "replyc":1, "ttl":4, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900913, "usec":315254}, "ttl":57, "rtt":22.572, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2600:803:150f::4a", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:4888:36:1002:3a2:1:0:1", "probes":[{"tx":{"sec":1567900913, "usec":543335}, "replyc":1, "ttl":5, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900913, "usec":568980}, "ttl":56, "rtt":25.645, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":793793}, "replyc":1, "ttl":5, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900913, "usec":816848}, "ttl":56, "rtt":23.055, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":43821}, "replyc":1, "ttl":5, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900914, "usec":72827}, "ttl":56, "rtt":29.006, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":294820}, "replyc":1, "ttl":5, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900914, "usec":320815}, "ttl":56, "rtt":25.995, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":545802}, "replyc":1, "ttl":5, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900914, "usec":568924}, "ttl":56, "rtt":23.122, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":796839}, "replyc":1, "ttl":5, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900914, "usec":824735}, "ttl":56, "rtt":27.896, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:4888:36:1002:3a2:1:0:1", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:4888:3f:6092:3a2:26:0:1", "probes":[{"tx":{"sec":1567900915, "usec":46897}, "replyc":1, "ttl":6, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900915, "usec":69996}, "ttl":245, "rtt":23.099, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":297455}, "replyc":1, "ttl":6, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900915, "usec":320524}, "ttl":245, "rtt":23.069, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":547737}, "replyc":1, "ttl":6, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900915, "usec":570899}, "ttl":245, "rtt":23.162, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":798167}, "replyc":1, "ttl":6, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900915, "usec":821218}, "ttl":245, "rtt":23.051, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900916, "usec":55367}, "replyc":1, "ttl":6, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900916, "usec":78485}, "ttl":245, "rtt":23.118, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900916, "usec":306410}, "replyc":1, "ttl":6, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900916, "usec":329419}, "ttl":245, "rtt":23.009, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:4888:3f:6092:3a2:26:0:1", "q_ttl":1, "linkc":1}]}
	{"type":"cycle-stop", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "stop_time":1566691541}
	`
	_, err := parser.ParseJSON("20190825T000138Z_ndt-plh7v_1566050090_000000000004D64D.jsonl", []byte(testStr), "", "")
	if err != nil {
		t.Fatalf("Err during json parsing %v", err)
	}
}

func TestParseJsonComplex(t *testing.T) {
	testStr := `{"UUID":"ndt-plh7v_1566050090_000000000004D60F","TracerouteCallerVersion":"bc092be","CachedResult":true,"CachedUUID":"ndt-w6lxg_1565921414_00000000000038C0"}
	{"type":"cycle-start", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "start_time":1566691268}
	{"type":"tracelb", "version":"0.1", "userid":0, "method":"icmp-echo", "src":"2001:550:1b01:1:e41d:2d00:151:f6c0", "dst":"2600:1009:b013:1a59:c369:b528:98fd:ab43", "start":{"sec":1567900908, "usec":729543, "ftime":"2019-09-08 00:01:48"}, "probe_size":60, "firsthop":1, "attempts":3, "confidence":95, "tos":0, "gaplimit":3, "wait_timeout":5, "wait_probe":250, "probec":85, "probec_max":3000, "nodec":6, "linkc":6, "nodes":[{"addr":"2001:550:1b01:1::1", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:550:3::1ca", "probes":[{"tx":{"sec":1567900908, "usec":979595}, "replyc":1, "ttl":2, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900909, "usec":16398}, "ttl":63, "rtt":36.803, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":229642}, "replyc":1, "ttl":2, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900909, "usec":229974}, "ttl":63, "rtt":0.332, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":480242}, "replyc":1, "ttl":2, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900909, "usec":480571}, "ttl":63, "rtt":0.329, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":730987}, "replyc":1, "ttl":2, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900909, "usec":731554}, "ttl":63, "rtt":0.567, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900909, "usec":982029}, "replyc":1, "ttl":2, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900909, "usec":982358}, "ttl":63, "rtt":0.329, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":232994}, "replyc":1, "ttl":2, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900910, "usec":234231}, "ttl":63, "rtt":1.237, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:550:3::1ca", "q_ttl":1, "linkc":1, "links":[[{"addr":"2600:803::79", "probes":[{"tx":{"sec":1567900910, "usec":483606}, "replyc":1, "ttl":3, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900910, "usec":500939}, "ttl":58, "rtt":17.333, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":734394}, "replyc":1, "ttl":3, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900910, "usec":752612}, "ttl":58, "rtt":18.218, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900910, "usec":985425}, "replyc":1, "ttl":3, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900911, "usec":6498}, "ttl":58, "rtt":21.073, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":235481}, "replyc":1, "ttl":3, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900911, "usec":252800}, "ttl":58, "rtt":17.319, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":486164}, "replyc":1, "ttl":3, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900911, "usec":503522}, "ttl":58, "rtt":17.358, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900911, "usec":737096}, "replyc":1, "ttl":3, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900911, "usec":760439}, "ttl":58, "rtt":23.343, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2600:803::79", "q_ttl":1, "linkc":1, "links":[[{"addr":"2600:803:150f::4a", "probes":[{"tx":{"sec":1567900911, "usec":987801}, "replyc":1, "ttl":4, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900912, "usec":10282}, "ttl":57, "rtt":22.481, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":238227}, "replyc":1, "ttl":4, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900912, "usec":262270}, "ttl":57, "rtt":24.043, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":539699}, "replyc":1, "ttl":4, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900912, "usec":562078}, "ttl":57, "rtt":22.379, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900912, "usec":789753}, "replyc":1, "ttl":4, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900912, "usec":812145}, "ttl":57, "rtt":22.392, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":42261}, "replyc":1, "ttl":4, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900913, "usec":64678}, "ttl":57, "rtt":22.417, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":292682}, "replyc":1, "ttl":4, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900913, "usec":315254}, "ttl":57, "rtt":22.572, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2600:803:150f::4a", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:4888:36:1002:3a2:1:0:1", "probes":[{"tx":{"sec":1567900913, "usec":543335}, "replyc":1, "ttl":5, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900913, "usec":568980}, "ttl":56, "rtt":25.645, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900913, "usec":793793}, "replyc":1, "ttl":5, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900913, "usec":816848}, "ttl":56, "rtt":23.055, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":43821}, "replyc":1, "ttl":5, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900914, "usec":72827}, "ttl":56, "rtt":29.006, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":294820}, "replyc":1, "ttl":5, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900914, "usec":320815}, "ttl":56, "rtt":25.995, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":545802}, "replyc":1, "ttl":5, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900914, "usec":568924}, "ttl":56, "rtt":23.122, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900914, "usec":796839}, "replyc":1, "ttl":5, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900914, "usec":824735}, "ttl":56, "rtt":27.896, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:4888:36:1002:3a2:1:0:1", "q_ttl":1, "linkc":1, "links":[[{"addr":"2001:4888:3f:6092:3a2:26:0:1", "probes":[{"tx":{"sec":1567900915, "usec":46897}, "replyc":1, "ttl":6, "attempt":0, "flowid":1, "replies":[{"rx":{"sec":1567900915, "usec":69996}, "ttl":245, "rtt":23.099, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":297455}, "replyc":1, "ttl":6, "attempt":0, "flowid":2, "replies":[{"rx":{"sec":1567900915, "usec":320524}, "ttl":245, "rtt":23.069, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":547737}, "replyc":1, "ttl":6, "attempt":0, "flowid":3, "replies":[{"rx":{"sec":1567900915, "usec":570899}, "ttl":245, "rtt":23.162, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900915, "usec":798167}, "replyc":1, "ttl":6, "attempt":0, "flowid":4, "replies":[{"rx":{"sec":1567900915, "usec":821218}, "ttl":245, "rtt":23.051, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900916, "usec":55367}, "replyc":1, "ttl":6, "attempt":0, "flowid":5, "replies":[{"rx":{"sec":1567900916, "usec":78485}, "ttl":245, "rtt":23.118, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]},{"tx":{"sec":1567900916, "usec":306410}, "replyc":1, "ttl":6, "attempt":0, "flowid":6, "replies":[{"rx":{"sec":1567900916, "usec":329419}, "ttl":245, "rtt":23.009, "icmp_type":3, "icmp_code":0, "icmp_q_tos":0, "icmp_q_ttl":1}]}]}]]},{"addr":"2001:4888:3f:6092:3a2:26:0:1", "q_ttl":1, "linkc":1, "links":[[{"addr":"*"}],[{"addr":"*"}],]}]}
	{"type":"cycle-stop", "list_name":"/tmp/scamperctrl:51803", "id":1, "hostname":"ndt-plh7v", "stop_time":1566691541}
	`
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

	protocol, dest_ip, server_ip, err = parser.ParseFirstLine("traceroute [(64.86.132.76:33461) -> (2001:0db8:85a3:0000:0000:8a2e:0370:7334:53849)], protocol icmp, algo exhaustive, duration 19 s")
	if dest_ip != "2001:0db8:85a3:0000:0000:8a2e:0370:7334" || server_ip != "64.86.132.76" || protocol != "icmp" || err != nil {
		t.Errorf("Error in parsing the first line!\n")
		return
	}

	protocol, dest_ip, server_ip, err = parser.ParseFirstLine("Exception : [ERROR](Probe.cc, 109)Can't send the probe : Invalid argument")
	if err == nil {
		t.Errorf("Should return error for err message on the first line!\n")
		return
	}

	protocol, dest_ip, server_ip, err = parser.ParseFirstLine("traceroute to 35.243.216.203 (35.243.216.203), 30 hops max, 30 bytes packets")
	if err == nil {
		t.Errorf("Should return error for unknown first line format!\n")
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

func TestJSONParser(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/PT/20190927T070859Z_ndt-qtfh8_1565996043_0000000000003B64.jsonl")
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	ptTest, err := parser.ParseJSON("20190927T070859Z_ndt-qtfh8_1565996043_0000000000003B64.jsonl", []byte(rawData), "", "")
	if err != nil {
		t.Fatalf(err.Error())
	}

	if ptTest.UUID != "ndt-qtfh8_1565996043_0000000000003B64" {
		t.Fatalf("UUID parsing error %s", ptTest.UUID)
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
	pt := parser.NewPTParser(ins, &fakeAnnotator{})
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

func TestPTInserterLastTest(t *testing.T) {
	ins := newInMemoryInserter()
	pt := parser.NewPTParser(ins, &fakeAnnotator{})
	rawData, err := ioutil.ReadFile("testdata/PT/20130524T00:04:44Z_ALL5729.paris")
	if err != nil {
		t.Fatalf("cannot read testdata.")
	}
	meta := map[string]bigquery.Value{"filename": "gs://fake-bucket/fake-archive.tgz"}
	err = pt.ParseAndInsert(meta, "testdata/PT/20130524T00:04:44Z_ALL5729.paris", rawData)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if pt.NumRowsForTest() != 0 {
		fmt.Println(pt.NumRowsForTest())
		t.Fatalf("The data is not inserted, in buffer now.")
	}
	pt.Flush()

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
		t.Fatalf("Number of tests in buffer not correct, expect 0, actually %d.", ins.RowsInBuffer())
	}
}

func TestPTEmptyTest(t *testing.T) {
	rawData, err := ioutil.ReadFile("testdata/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris")
	if err != nil {
		fmt.Println("cannot load test data")
		return
	}
	_, parseErr := parser.Parse(nil, "testdata/20180201T07:57:37Z-125.212.217.215-56622-208.177.76.115-9100.paris", "", rawData, "pt-daily")
	if parseErr == nil {
		t.Fatal(parseErr)
	}
}
