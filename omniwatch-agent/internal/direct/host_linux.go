//go:build linux

package direct

import "syscall"

func diskUsedPct() float64 {
	var fs syscall.Statfs_t
	if err := syscall.Statfs("/", &fs); err != nil {
		return 0
	}
	if fs.Blocks == 0 {
		return 0
	}
	used := fs.Blocks - fs.Bavail
	return float64(used) / float64(fs.Blocks) * 100
}
