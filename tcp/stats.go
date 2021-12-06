package tcp

import (
	"fmt"
	"math"
)

type LinReg struct {
	N, X, XX, XY, Y, YY float64
}

func (l *LinReg) Add(x, y float64) {
	l.N += 1
	l.X += x
	l.XX += x * x
	l.XY += x * y
	l.Y += y
	l.YY += y * y
}

func (l *LinReg) sxx() float64 {
	// ∑(x-<x>)^2 = ∑(x^2) - (∑x)^2/n
	// sxx = sum(dx^2) = L.XX - L.X * L.X/L.N
	return l.XX - l.X*l.X/l.N
}
func (l *LinReg) sxy() float64 {
	return l.XY - l.X*l.Y/l.N
}
func (l *LinReg) syy() float64 {
	return l.YY - l.Y*l.Y/l.N
}

func (l *LinReg) MeanX() float64 {
	return l.X / l.N
}

func (l *LinReg) Slope() float64 {
	return (l.N*l.XY - l.X*l.Y) / (l.N*l.XX - l.X*l.X)
}

func (l *LinReg) Variance() float64 {
	return (l.N*l.XX - l.X*l.X) / (l.N * l.N)
}

// This is the standard deviation of the X values
func (l *LinReg) Stddev() float64 {
	return math.Sqrt(l.Variance())
}

// Residuals
func (l *LinReg) RSS() float64 {
	return l.syy() * (1 - l.sxy()*l.sxy()/(l.sxx()*l.syy()))
}

func (l *LinReg) R2() float64 {
	return 1 - l.RSS()/l.syy()
}

func (l *LinReg) YVar() float64 {
	return l.RSS() / l.N
}

func (l *LinReg) Estimate(x float64) float64 {
	return l.Y/l.N + l.Slope()*(x-l.X/l.N)
}

func (l *LinReg) String() string {
	return fmt.Sprintf("Y = %9.5f X + %9.5f, %d points, R2 = %8.6f",
		l.Slope(), l.Y/l.N-l.Slope()*l.X/l.N, int(l.N), l.R2())
}

type LogHistogram struct {
	BinCounts     []int
	binBounds     []float64 // The upper bounds of each bin
	min           float64   // minimum value in the histogram
	factor        float64   // multiplier between bin values
	binsPerDecade float64   // number of bins per decade
	count         int       // total number of samples
}

func (s *LogHistogram) Index(val float64) int {
	// invariant: lower <= target <= upper
	lower := 0
	upper := len(s.binBounds) - 1

	for lower+8 < upper {
		mid := (upper + lower) / 2
		if val < s.binBounds[mid] {
			upper = mid
		} else {
			lower = mid
		}
	}

	for ; lower < upper; lower++ {
		if val < s.binBounds[lower] {
			break
		}
	}
	return lower
}

func (s *LogHistogram) SlowIndex(val float64) int {
	ix := int(math.Round(s.binsPerDecade * math.Log10(val/s.min)))
	if ix < 0 {
		return 0
	}
	if ix >= len(s.BinCounts) {
		return len(s.BinCounts) - 1
	}
	return ix
}

// Add updates the histogram with the given value.
func (s *LogHistogram) Add(dt float64) {
	i := s.Index(dt)

	if i < 0 {
		s.BinCounts[0]++
	} else if i >= len(s.BinCounts) {
		s.BinCounts[len(s.BinCounts)-1]++
	} else {
		s.BinCounts[i]++
	}
}

// BinValue returns the value of the bin at the given index.
// It should also be binBounds[i]/sqrt(factor).
func (s *LogHistogram) BinValue(i int) float64 {
	return s.min * math.Pow(10.0, (float64(i)/float64(s.binsPerDecade)))
}

func (s *LogHistogram) Stats(useDelay bool) (float64, float64, float64) {
	count := 0
	p05 := 0.0
	p50 := 0.0
	p95 := 0.0
	binVal := s.min

	if !useDelay {
		for _, n := range s.BinCounts {
			count += n
			if p05 == 0 && count > s.count/20 {
				p05 = binVal
			}
			if p50 == 0 && count > s.count/2 {
				p50 = binVal
			}
			if p95 == 0 && count > 19*s.count/20 {
				p95 = binVal
			}
			binVal *= math.Pow(10.0, (1 / float64(s.binsPerDecade)))
		}
	} else {
		// TODO - this really should take real time into account.  Currently
		// the histogram is scaled in ack packets, instead of time.
		// Instead, we could increment float64 bins with samples uniform in
		// time.
		total := 0.0
		for i, n := range s.BinCounts {
			total += float64(n) * s.BinValue(i)
		}
		running := 0.0
		for i, n := range s.BinCounts {
			running += float64(n) * s.BinValue(i)
			if p05 == 0 && running > total/20 {
				p05 = s.BinValue(i)
			}
			if p50 == 0 && running > total/2 {
				p50 = s.BinValue(i)
			}
			if p95 == 0 && running > 19*total/20 {
				p95 = s.BinValue(i)
			}
		}
	}
	return p05, p50, p95

}

func NewLogHistogram(min float64, max float64, binsPerDecade float64) (LogHistogram, error) {
	if min <= 0 || min >= max {
		return LogHistogram{}, fmt.Errorf("min must be > 0 and < max")
	}
	numBins := 1 + int(math.Round(math.Log10(max/min))*binsPerDecade)
	binBounds := make([]float64, numBins)
	factor := math.Pow(10.0, (1 / float64(binsPerDecade)))
	next := min * math.Sqrt(factor) // upper bound of first bin
	for i := 0; i < numBins; i++ {
		binBounds[i] = next
		next *= factor
	}
	return LogHistogram{
		BinCounts:     make([]int, numBins),
		binBounds:     binBounds,
		min:           min,
		factor:        factor,
		binsPerDecade: binsPerDecade,
	}, nil
}
