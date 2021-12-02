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
