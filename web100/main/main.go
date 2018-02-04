package main

import (
	"io/ioutil"
	"log"
	"testing"

	"net/http"
	_ "net/http/pprof"

	"github.com/m-lab/etl/web100"
)

func BenchmarkSliceInt(b *testing.B) {
	b.StopTimer()
	//s2cName := `20090401T09:01:09.490730000Z_131.169.137.246:14881.c2s_snaplog`
	//s2cName := `20170430T11:54:26.658288000Z_p508486E9.dip0.t-ipconnect.de:53088.s2c_snaplog`
	s2cName := `20090601T22:19:19.325928000Z-75.133.69.98:60631.s2c_snaplog`
	data, err := ioutil.ReadFile(`testdata/` + s2cName)
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

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		slog.SliceIntField("SmoothedRTT", indices)
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	s2cName := `20090601T22:19:19.325928000Z-75.133.69.98:60631.s2c_snaplog`
	data, err := ioutil.ReadFile(`testdata/` + s2cName)
	if err != nil {
		log.Fatalf(err.Error())
	}
	slog, err := web100.NewSnapLog(data)
	if err != nil {
		log.Fatalf(err.Error())
	}
	indices, err := slog.ChangeIndices("SmoothedRTT")
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.Println(len(indices))

	for i := 0; i < 1000000; i++ {
		slog.SliceIntField("SmoothedRTT", indices)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}
}
