package web100_test

import (
	//"fmt"
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/m-lab/etl/web100"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestValidation(t *testing.T) {
}

func TestHeaderParsing(t *testing.T) {
	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	slog, err := web100.NewSnapLog(c2sData)

	if err != nil {
		t.Fatal(err.Error())
	}
	if len(slog.Body.Fields) != 142 {
		log.Printf("%d %v\n", len(slog.Body.Fields), slog.Body)
		t.Error("Wrong number of fields.")
	}
	if slog.Body.RecordLength != 669 {
		log.Printf("Record length %d\n", slog.Body.RecordLength)
		t.Error("Wrong record length.")
	}

	if slog.LogTime != 1494337516 {
		t.Error("Incorrect LogTime.")
	}
	if err = slog.Validate(); err != nil {
		t.Error(err)
	}
}

type Saver struct {
}

func (s *Saver) SetString(name string, val string) {
	fmt.Printf("%s: %s\n", name, val)
}

func (s *Saver) SetInt64(name string, val int64) {
	fmt.Printf("%s: %d\n", name, val)
}

func TestSnapshotContent(t *testing.T) {
	c2sName := `20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	c2sData, err := ioutil.ReadFile(`testdata/` + c2sName)
	if err != nil {
		t.Fatalf(err.Error())
	}
	slog, err := web100.NewSnapLog(c2sData)
	if err != nil {
		t.Fatal(err.Error())
	}

	snapshot, err := slog.Snapshot(0)
	var saver Saver
	snapshot.SnapshotValues(&saver)

}
