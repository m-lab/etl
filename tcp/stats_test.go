package tcp_test

import (
	"log"
	"math/rand"
	"testing"

	"github.com/m-lab/etl/tcp"
)

func TestLinearRegression(t *testing.T) {
	lr := tcp.LinReg{}

	for i := 0.0; i < 1000.0; i++ {
		// +/- 25
		// RSS should be around 625?
		lr.Add(i+.5*rand.Float64(), 1000-2*i+100*(rand.Float64()-.5))
	}

	//log.Println(lr.Slope(), lr.Stddev(), lr.R2(), lr.RSS(), lr.YVar())
	for i := 0.0; i < 1000.0; i += 100.0 {
		log.Println(i, lr.Estimate(i))
	}
}

func TestLogHistogramIndex(t *testing.T) {
	h, _ := tcp.NewLogHistogram(1e-5, 1.0, 6)

	for i := 0; i < 1000000; i++ {
		v := rand.NormFloat64()
		v = v * v * v * v
		if h.Index(v) != h.SlowIndex(v) {
			t.Errorf("%g, %d != %d", v, h.Index(v), h.SlowIndex(v))
		}
		h.Add(v)
	}
}

func BenchmarkLogHistogram(b *testing.B) {
	h, _ := tcp.NewLogHistogram(1e-5, 1.0, 6)

	for i := 0; i < b.N; i++ {
		v := float64(i%100) / 100.0
		h.Add(v * v)
	}
}
