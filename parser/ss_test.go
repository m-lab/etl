package parser_test

import (
	"fmt"
	//"io/ioutil"
	//"reflect"
	"syscall"
	"testing"

	"github.com/m-lab/etl/parser"
	//"github.com/m-lab/etl/schema"
)

func TestExtractLogtimeFromFilename(t *testing.T) {
	log_time, _ := parser.ExtractLogtimeFromFilename("20170315T01:00:00Z_173.205.3.39_0.web100")
	if log_time != 1489539600 {
		fmt.Println(log_time)
		t.Fatalf("Do not extract log time correctly.")
	}
}

func TestParseIPFamily(t *testing.T) {
	if parser.ParseIPFamily("1.2.3.4") != syscall.AF_INET {
		t.Fatalf("Do not parse IPv4 address correctly.")
	}
}
