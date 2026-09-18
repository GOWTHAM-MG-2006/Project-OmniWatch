// OmniWatch — Agent
// Component: Beyla startup kernel/eBPF advisory check
// Phase: industry-ready (IND-6)
// Purpose: Verify host kernel >= 5.10 and eBPF availability at agent boot
// Inputs: Host kernel release string (/proc/sys/kernel/osrelease on Linux)
// Outputs: Advisory Result logged at boot; non-fatal, agent still runs receivers
package beyla

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// MinMajor/MinMinor is the minimum kernel version Beyla needs for eBPF
// auto-instrumentation.
const (
	MinMajor = 5
	MinMinor = 10
)

// MinVersion is the minimum supported kernel as a string.
const MinVersion = "5.10"

// Version is a parsed kernel release (major.minor.patch).
type Version struct {
	Major int
	Minor int
	Patch int
}

// String returns the dotted form of v.
func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Supported reports whether v meets the minimum kernel floor.
func (v Version) Supported() bool {
	if v.Major != MinMajor {
		return v.Major > MinMajor
	}
	return v.Minor >= MinMinor
}

// ParseKernelVersion parses strings like "5.15.0-91-generic", "6.8.0",
// "5.10" into a Version. Suffixes after '-' or '+' are ignored.
func ParseKernelVersion(s string) (Version, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Version{}, fmt.Errorf("empty kernel version string")
	}
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return Version{}, fmt.Errorf("unparsable kernel version %q: need at least major.minor", s)
	}
	nums := make([]int, 3)
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(strings.TrimSpace(parts[i]))
		if err != nil {
			return Version{}, fmt.Errorf("unparsable kernel version %q: %v", s, err)
		}
		nums[i] = n
	}
	return Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, nil
}

// remediationSteps are logged alongside any unsupported-kernel error.
func remediationSteps() string {
	return "remediation: upgrade node kernel to >= 5.10; " +
		"enable CONFIG_BPF/CONFIG_BPF_SYSCALL; " +
		"mount bpffs (mount -t bpf bpffs /sys/fs/bpf); " +
		"grant CAP_BPF/CAP_SYS_ADMIN (compose cap_add SYS_ADMIN, " +
		"K8s privileged:true on the beyla container only)"
}

// CheckKernelRelease validates a kernel release string against the floor.
// It returns nil when supported, otherwise an actionable error.
func CheckKernelRelease(release string) error {
	v, err := ParseKernelVersion(release)
	if err != nil {
		return fmt.Errorf("cannot verify kernel version (%v); %s", err, remediationSteps())
	}
	if !v.Supported() {
		return fmt.Errorf("kernel %s+ required for eBPF, current: %d.%d; %s",
			MinVersion, v.Major, v.Minor, remediationSteps())
	}
	return nil
}

// hostRelease returns the running kernel release, or "" when it cannot be
// determined (e.g. non-Linux hosts such as Windows/WSL2 control planes).
func hostRelease() string {
	if runtime.GOOS != "linux" {
		return ""
	}
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// ebpfAvailable reports whether the eBPF filesystem is present. On Linux this
// checks for a mounted bpffs; on other OSes eBPF is unavailable.
func ebpfAvailable() (bool, string) {
	if runtime.GOOS != "linux" {
		return false, fmt.Sprintf("GOOS=%s: eBPF requires Linux", runtime.GOOS)
	}
	fi, err := os.Stat("/sys/fs/bpf")
	if err != nil || !fi.IsDir() {
		return false, "bpffs not mounted at /sys/fs/bpf (mount -t bpf bpffs /sys/fs/bpf)"
	}
	return true, "bpffs mounted at /sys/fs/bpf"
}

// Result is the outcome of the advisory startup check.
type Result struct {
	Release       string // kernel release string, or "unknown"
	KernelOK      bool
	EBPFAvailable bool
	EBPFDetail    string
	Err           error // non-nil when kernel is below the floor
}

// Check runs the advisory startup check against the live host.
func Check() Result {
	r := Result{Release: hostRelease()}
	if r.Release == "" {
		r.Release = fmt.Sprintf("unknown (GOOS=%s)", runtime.GOOS)
		r.Err = fmt.Errorf("kernel %s+ required for eBPF, current: %s; %s",
			MinVersion, r.Release, remediationSteps())
	} else if err := CheckKernelRelease(r.Release); err != nil {
		r.Err = err
	} else {
		r.KernelOK = true
	}
	r.EBPFAvailable, r.EBPFDetail = ebpfAvailable()
	return r
}

// LogCheck runs Check and logs the outcome. It never fails: an unsupported
// host is advisory only — the agent still runs its receivers.
func LogCheck(logger *slog.Logger) Result {
	r := Check()
	if r.Err != nil {
		logger.Warn("beyla preflight: host kernel unsupported, eBPF traces degraded",
			"kernel_release", r.Release,
			"kernel_min", MinVersion,
			"ebpf_available", r.EBPFAvailable,
			"ebpf_detail", r.EBPFDetail,
			"error", r.Err)
		return r
	}
	logger.Info("beyla preflight: eBPF supported",
		"kernel_release", r.Release,
		"kernel_min", MinVersion,
		"ebpf_available", r.EBPFAvailable,
		"ebpf_detail", r.EBPFDetail)
	return r
}
