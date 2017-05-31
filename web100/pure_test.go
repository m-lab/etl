package web100_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/m-lab/etl/web100"
)

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
		fmt.Printf("%d %v\n", len(slog.Body.Fields), slog.Body)
		t.Error("Wrong number of fields.")
	}
	if slog.Body.RecordLength != 669 {
		fmt.Printf("Record length %d\n", slog.Body.RecordLength)
		t.Error("Wrong record length.")
	}
	fmt.Printf("%d %x\n", slog.Spec.RecordLength, slog.ConnSpecOffset)
	fmt.Printf("%d %x\n", slog.Body.RecordLength, slog.BodyOffset)
	fmt.Printf("%x\n", slog.Body.RecordLength+slog.BodyOffset)

	if slog.LogTime != 1494337516 {
		t.Error("Incorrect LogTime.")
	}
	if err = slog.Validate(); err != nil {
		t.Error(err)
	}
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

	_, err = slog.Snapshot(1)

}
