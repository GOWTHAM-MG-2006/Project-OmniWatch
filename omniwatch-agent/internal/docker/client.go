// OmniWatch — Agent
// Component: docker (Engine API client)
// Phase: real-application-telemetry (Item 2+3)
// Purpose: Talk to the Docker Engine API over the unix socket with stdlib
// only (no docker SDK dependency, keeps the static binary lean). Used by the
// app-log tailer and the container-stats poller.
// Inputs: Docker socket path (default /var/run/docker.sock)
// Outputs: Container list, demultiplexed container logs, one-shot stats
package docker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// apiVersion pins an ancient Engine API so old daemons (like the EC2 box's)
// accept every call used here (list, logs, one-shot stats all exist since v1.24).
const apiVersion = "v1.24"

// DefaultSocketPath is the standard Docker Engine unix socket.
const DefaultSocketPath = "/var/run/docker.sock"

// Container is the subset of /containers/json we need.
type Container struct {
	ID    string
	Names []string
	Image string
	State string
}

// Name returns the primary container name without the leading slash.
func (c Container) Name() string {
	if len(c.Names) == 0 {
		return c.ID
	}
	return strings.TrimPrefix(c.Names[0], "/")
}

// Client queries one Docker daemon over its unix socket.
type Client struct {
	socket string
	http   *http.Client
}

// New builds a Client dialing the given socket path. Empty path means the
// default socket. No connection is opened until the first call.
func New(socketPath string) *Client {
	if strings.TrimSpace(socketPath) == "" {
		socketPath = DefaultSocketPath
	}
	sock := socketPath
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", sock)
		},
	}
	return &Client{socket: socketPath, http: &http.Client{Transport: transport, Timeout: 15 * time.Second}}
}

// SocketPath reports the configured socket (for preflight diagnostics).
func (c *Client) SocketPath() string { return c.socket }

func (c *Client) get(path string, query url.Values) ([]byte, error) {
	u := "http://localhost/" + apiVersion + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	resp, err := c.http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("docker: GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("docker: read %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("docker: GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

// List returns all containers (running or not). Callers filter by state/name.
func (c *Client) List() ([]Container, error) {
	raw, err := c.get("/containers/json", url.Values{"all": {"true"}})
	if err != nil {
		return nil, err
	}
	var items []struct {
		ID     string   `json:"Id"`
		Names  []string `json:"Names"`
		Image  string   `json:"Image"`
		State  string   `json:"State"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("docker: decode container list: %w", err)
	}
	out := make([]Container, 0, len(items))
	for _, it := range items {
		out = append(out, Container{ID: it.ID, Names: it.Names, Image: it.Image, State: it.State})
	}
	return out, nil
}

// LogLine is one timestamped container log line. TimestampUnix is 0 when the
// daemon did not prefix a parseable timestamp.
type LogLine struct {
	TimestampUnix int64
	Message       string
}

// Logs fetches stdout+stderr for one container. sinceUnix filters server-side
// (0 = no filter); tail caps the returned lines ("all" when <= 0 — callers
// should always pass a positive cap). Timestamps are requested so the tailer
// can track per-container progress.
func (c *Client) Logs(name string, sinceUnix int64, tail int) ([]LogLine, error) {
	q := url.Values{
		"stdout":     {"true"},
		"stderr":     {"true"},
		"timestamps": {"true"},
	}
	if sinceUnix > 0 {
		q.Set("since", fmt.Sprintf("%d", sinceUnix))
	}
	if tail > 0 {
		q.Set("tail", fmt.Sprintf("%d", tail))
	} else {
		q.Set("tail", "all")
	}
	raw, err := c.get("/containers/"+name+"/logs", q)
	if err != nil {
		return nil, err
	}
	return splitLogLines(demux(raw)), nil
}

// demux strips Docker's 8-byte multiplex framing (stream,0,0,0,size-BE32).
// Non-TTY containers always frame; if the payload is not framed it is
// returned as-is so plain output still works.
func demux(raw []byte) []byte {
	if len(raw) < 8 {
		return raw
	}
	// Framed payloads start with a stream byte 0/1/2 followed by 3 zero bytes.
	if raw[1] != 0 || raw[2] != 0 || raw[3] != 0 || raw[0] > 2 {
		return raw
	}
	var out []byte
	for len(raw) >= 8 {
		size := binary.BigEndian.Uint32(raw[4:8])
		raw = raw[8:]
		if uint64(len(raw)) < uint64(size) {
			break // truncated frame: stop rather than emit garbage
		}
		out = append(out, raw[:size]...)
		raw = raw[size:]
	}
	if out == nil {
		return raw
	}
	return out
}

// splitLogLines splits payload lines and strips the "RFC3339Nano message"
// timestamp prefix the daemon adds when timestamps=true.
func splitLogLines(payload []byte) []LogLine {
	var out []LogLine
	for _, ln := range strings.Split(string(payload), "\n") {
		ln = strings.TrimRight(ln, "\r")
		if ln == "" {
			continue
		}
		var ll LogLine
		if i := strings.IndexByte(ln, ' '); i > 0 {
			if ts, err := time.Parse(time.RFC3339Nano, ln[:i]); err == nil {
				ll.TimestampUnix = ts.Unix()
				ln = ln[i+1:]
			}
		}
		ll.Message = ln
		out = append(out, ll)
	}
	return out
}

// ContainerStats is the one-shot resource snapshot for a container.
type ContainerStats struct {
	Name         string
	CPUPercent   float64
	MemUsedBytes uint64
	MemLimitBytes uint64
}

// MemUsedPercent returns 0 when the daemon reports no limit.
func (s ContainerStats) MemUsedPercent() float64 {
	if s.MemLimitBytes == 0 {
		return 0
	}
	return float64(s.MemUsedBytes) / float64(s.MemLimitBytes) * 100
}

// Stats fetches a single non-streaming sample for one container.
func (c *Client) Stats(name string) (ContainerStats, error) {
	var st ContainerStats
	raw, err := c.get("/containers/"+name+"/stats", url.Values{"stream": {"false"}, "one-shot": {"true"}})
	if err != nil {
		return st, err
	}
	var s struct {
		CPUStats struct {
			CPUUsage struct {
				TotalUsage  uint64   `json:"total_usage"`
				PercpuUsage []uint64 `json:"percpu_usage"`
			} `json:"cpu_usage"`
			SystemCPUUsage uint64 `json:"system_cpu_usage"`
			OnlineCPUs     uint64 `json:"online_cpus"`
		} `json:"cpu_stats"`
		PreCPUStats struct {
			CPUUsage struct {
				TotalUsage uint64 `json:"total_usage"`
			} `json:"cpu_usage"`
			SystemCPUUsage uint64 `json:"system_cpu_usage"`
		} `json:"precpu_stats"`
		MemoryStats struct {
			Usage uint64 `json:"usage"`
			Limit uint64 `json:"limit"`
		} `json:"memory_stats"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return st, fmt.Errorf("docker: decode stats for %s: %w", name, err)
	}
	st.Name = name
	st.MemUsedBytes = s.MemoryStats.Usage
	st.MemLimitBytes = s.MemoryStats.Limit
	cpuDelta := float64(s.CPUStats.CPUUsage.TotalUsage) - float64(s.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(s.CPUStats.SystemCPUUsage) - float64(s.PreCPUStats.SystemCPUUsage)
	numCPUs := float64(len(s.CPUStats.CPUUsage.PercpuUsage))
	if numCPUs == 0 {
		numCPUs = float64(s.CPUStats.OnlineCPUs)
	}
	if cpuDelta > 0 && sysDelta > 0 && numCPUs > 0 {
		st.CPUPercent = cpuDelta / sysDelta * numCPUs * 100
	}
	return st, nil
}
