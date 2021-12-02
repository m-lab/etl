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

	log.Println(lr.Slope(), lr.Stddev(), lr.R2(), lr.RSS(), lr.YVar())
	for i := 0.0; i < 1000.0; i += 100.0 {
		log.Println(i, lr.Estimate(i))
	}

}
