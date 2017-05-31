package web100_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/m-lab/etl/web100"
)

func TestValidation(t *testing.T) {
}

func TestWeb100(t *testing.T) {
	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	log, err := web100.NewSnapLog(c2sData)

	if err != nil {
		t.Fatal(err.Error())
	}
	if len(log.Body.Fields) != 142 {
		fmt.Printf("%d %v\n", len(log.Body.Fields), log.Body)
		t.Error("Wrong number of fields.")
	}
	if log.Body.RecordLength != 645 {
		fmt.Printf("Record length %d\n", log.Body.RecordLength)
		t.Error("Wrong record length.")
	}
	fmt.Println(log.Buf.Len())

}
