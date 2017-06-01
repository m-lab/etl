package web100_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"testing"

	"github.com/m-lab/etl/web100"
)

var legacyNames map[string]string

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		panic("tcp-kis.txt not found")
	}
	b := bytes.NewBuffer(data)

	legacyNames, err = web100.ParseWeb100Definitions(b)
	if err != nil {
		panic("error parsing tcp-kis.txt")
	}
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

type SimpleSaver struct {
	Integers map[string]int64
	Strings  map[string]string
}

func NewSimpleSaver() SimpleSaver {
	return SimpleSaver{make(map[string]int64), make(map[string]string)}
}

func (s SimpleSaver) SetString(name string, val string) {
	s.Strings[name] = val
}

func (s SimpleSaver) SetInt64(name string, val int64) {
	s.Integers[name] = val
}

func OldRead(n int) SimpleSaver {
	c2sName := `testdata/20170509T13:45:13.590210000Z_eb.measurementlab.net:48716.c2s_snaplog`
	w, err := web100.Open(c2sName, legacyNames)
	if err != nil {
		panic("Couldn't open snaplog file")
	}
	defer w.Close()

	for count := 0; count < 2100; count++ {
		err := w.Next()
		if err != nil {
			panic("Next failed")
		}
	}
	saver := NewSimpleSaver()
	err = w.SnapshotValues(saver)
	return saver
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

	snapshot, err := slog.Snapshot(1000)
	saver := NewSimpleSaver()
	snapshot.SnapshotValues(&saver)

	old := OldRead(1000)
	if !reflect.DeepEqual(old, saver) {
		t.Error("Does not match old output")
		fmt.Printf("%+v\n", saver)
		fmt.Printf("%+v\n", old)
	}
}
