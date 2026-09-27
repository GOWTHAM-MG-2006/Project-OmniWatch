// OmniWatch — Agent
// Component: direct host stats
// Phase: exporter-importer-split
// Purpose: Dependency-free host readings for the direct heartbeat (Linux
// /proc; every reader degrades to zero/false anywhere else so unit tests
// pass on any OS and no fake data is ever emitted).
// Inputs: /proc filesystem (Linux only)
// Outputs: HostStats (load, memory, disk, uptime, GC pause)
package direct

import (
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HostStats carries one sampling of host readings. Zero values mean
// "unavailable on this platform" — callers must not fabricate rows.
type HostStats struct {
	Host        string
	Load1       float64
	MemUsedPct  float64
	DiskUsedPct float64
	UptimeS     int64
}

func readHostStats() HostStats {
	st := HostStats{}
	if host, err := os.Hostname(); err == nil {
		st.Host = host
	}
	if runtime.GOOS != "linux" {
		return st
	}
	st.Load1 = readLoad1()
	st.MemUsedPct = readMemUsedPct()
	st.DiskUsedPct = readDiskUsedPct()
	st.UptimeS = readUptimeS()
	return st
}

// SelfProfile is one live snapshot of the agent's own runtime, sourced from
// runtime/pprof (goroutine profile) plus MemStats. GCPauseIntervalMs is the
// GC pause that actually elapsed since the previous snapshot — a genuine
// interval-scoped duration, never a cumulative counter relabeled as a span.
type SelfProfile struct {
	GCPauseIntervalMs float64
	Goroutines        int
}

var (
	gcPauseMu       sync.Mutex
	lastPauseTotalN uint64
	lastPauseInit   bool
)

// selfProfile takes a live runtime snapshot. The first call only arms the
// GC-pause baseline and reports 0 rather than emitting the process-lifetime
// cumulative pause as an interval value. Falls back to NumGoroutine only if
// the pprof table is somehow unavailable.
func selfProfile() SelfProfile {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	goroutines := 0
	if p := pprof.Lookup("goroutine"); p != nil {
		goroutines = p.Count()
	}
	if goroutines <= 0 {
		goroutines = runtime.NumGoroutine()
	}
	gcPauseMu.Lock()
	total := mem.PauseTotalNs
	var intervalMs float64
	if lastPauseInit {
		intervalMs = float64(total-lastPauseTotalN) / 1e6
	} else {
		lastPauseInit = true
	}
	lastPauseTotalN = total
	gcPauseMu.Unlock()
	return SelfProfile{GCPauseIntervalMs: intervalMs, Goroutines: goroutines}
}

func readLoad1() float64 {
	raw, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw))
	if len(fields) == 0 {
		return 0
	}
	f, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return f
}

func readMemUsedPct() float64 {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	var total, avail float64
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(f[1], 64)
		if err != nil {
			continue
		}
		switch f[0] {
		case "MemTotal:":
			total = v
		case "MemAvailable:":
			avail = v
		}
	}
	if total <= 0 {
		return 0
	}
	return (total - avail) / total * 100
}

func readDiskUsedPct() float64 {
	return diskUsedPct()
}

func readUptimeS() int64 {
	raw, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(raw))
	if len(f) == 0 {
		return 0
	}
	secs, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return 0
	}
	return int64(secs)
}

var _ = time.Second // keep time import if readers evolve
