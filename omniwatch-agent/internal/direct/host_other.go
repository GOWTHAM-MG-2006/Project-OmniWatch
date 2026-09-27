//go:build !linux

package direct

func diskUsedPct() float64 { return 0 }
