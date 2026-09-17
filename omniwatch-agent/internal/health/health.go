// OmniWatch — Agent
// Component: health
// Phase: 1
// Purpose: Health and readiness HTTP endpoints with collector status tracking
// Inputs: Listen address, collector status updates via SetCollectorStatus
// Outputs: HTTP /health and /ready responses
package health

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Collector statuses tracked by the health server.
const (
	CollectorStarting = "starting"
	CollectorRunning  = "running"
	CollectorStopped  = "stopped"
	CollectorDegraded = "degraded"
)

// Server exposes liveness and readiness probes and tracks collector state.
// /health always returns 200 OK while the process is alive.
// /ready returns 200 only once the collector reports running; otherwise 503.
type Server struct {
	mu              sync.RWMutex
	collectorStatus string
	ready           bool
}

// NewServer returns a Server with the collector initially in starting state.
func NewServer() *Server {
	return &Server{collectorStatus: CollectorStarting}
}

// SetCollectorStatus records the collector lifecycle state. Passing
// CollectorRunning also marks the server ready; any other state marks it
// not-ready so the /ready probe reflects collection health.
func (s *Server) SetCollectorStatus(status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectorStatus = status
	s.ready = status == CollectorRunning
}

// CollectorStatus returns the last recorded collector state.
func (s *Server) CollectorStatus() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.collectorStatus
}

// statusPayload is the JSON body served by both probes.
type statusPayload struct {
	Status          string `json:"status"`
	CollectorStatus string `json:"collector_status"`
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	collector := s.collectorStatus
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, statusPayload{Status: "ok", CollectorStatus: collector})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	collector := s.collectorStatus
	ready := s.ready
	s.mu.RUnlock()
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, statusPayload{Status: "not_ready", CollectorStatus: collector})
		return
	}
	writeJSON(w, http.StatusOK, statusPayload{Status: "ready", CollectorStatus: collector})
}

// Handler returns the HTTP mux serving /health and /ready.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v statusPayload) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
