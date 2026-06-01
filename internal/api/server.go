package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
)

// HandlerRegistry is a function that registers routes onto a mux.
type HandlerRegistry func(mux *http.ServeMux)

// Server setup API server.
type Server struct {
	svr    *http.Server
	router *http.ServeMux
}

// NewServer creates a new HTTP server listening on the given port.
func NewServer(port int) *Server {
	mux := http.NewServeMux()

	return &Server{
		svr: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
		router: mux,
	}
}

// RegisterRoutes calls each HandlerRegistry to mount routes onto the server mux.
func (s *Server) RegisterRoutes(registries ...HandlerRegistry) {
	for _, r := range registries {
		r(s.router)
	}
}

// Listen starts accepting connections. It returns nil when the server is
// stopped via Shutdown (http.ErrServerClosed is treated as a clean exit).
func (s *Server) Listen() error {
	log.Println("HTTP API listening on :8080")
	if err := s.svr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http listen failed: %w", err)
	}
	return nil
}

// Shutdown gracefully drains in-flight requests within the given context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.svr.Shutdown(ctx)
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a plain-text error response.
func writeError(w http.ResponseWriter, status int, msg string) {
	http.Error(w, msg, status)
}

// newID generates a new random UUID v4 task ID.
func newID() string {
	return uuid.New().String()
}
