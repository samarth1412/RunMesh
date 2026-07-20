package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron/v3"
	"github.com/runmesh/runmesh/internal/auth"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/workflow"
	openapispec "github.com/runmesh/runmesh/openapi"
)

const maxBodyBytes = 1 << 20

type Server struct {
	Store         *storage.Store
	LeaseDuration time.Duration
	InternalToken string
	Auth          func(http.Handler) http.Handler
	Requests      *prometheus.HistogramVec
}

func New(store *storage.Store, lease time.Duration, internalToken string, authMiddleware func(http.Handler) http.Handler) *Server {
	req := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "api_request_duration_seconds", Help: "Public API request latency", Buckets: prometheus.DefBuckets}, []string{"method", "route", "status"})
	prometheus.MustRegister(req)
	return &Server{Store: store, LeaseDuration: lease, InternalToken: internalToken, Auth: authMiddleware, Requests: req}
}

func (s *Server) Handler() http.Handler {
	root := http.NewServeMux()
	public := http.NewServeMux()
	public.HandleFunc("POST /v1/workflows", auth.RequireRole("developer", s.createWorkflow))
	public.HandleFunc("GET /v1/workflows", auth.RequireRole("viewer", s.listWorkflows))
	public.HandleFunc("GET /v1/workflows/{id}", auth.RequireRole("viewer", s.getWorkflow))
	public.HandleFunc("POST /v1/workflows/{id}/versions", auth.RequireRole("developer", s.createVersion))
	public.HandleFunc("POST /v1/workflows/{id}/runs", auth.RequireRole("operator", s.createRun))
	public.HandleFunc("GET /v1/runs", auth.RequireRole("viewer", s.listRuns))
	public.HandleFunc("GET /v1/runs/{id}", auth.RequireRole("viewer", s.getRun))
	public.HandleFunc("POST /v1/runs/{id}/cancel", auth.RequireRole("operator", s.cancelRun))
	public.HandleFunc("POST /v1/runs/{id}/retry", auth.RequireRole("operator", s.retryRun))
	public.HandleFunc("GET /v1/runs/{id}/events", auth.RequireRole("viewer", s.events))
	public.HandleFunc("GET /v1/workers", auth.RequireRole("viewer", s.workers))
	public.HandleFunc("GET /v1/dead-letter", auth.RequireRole("operator", s.deadLetter))
	public.HandleFunc("POST /v1/dead-letter/{id}/replay", auth.RequireRole("operator", s.replay))
	public.HandleFunc("POST /v1/schedules", auth.RequireRole("developer", s.createSchedule))
	public.HandleFunc("PATCH /v1/schedules/{id}", auth.RequireRole("developer", s.patchSchedule))
	public.HandleFunc("DELETE /v1/schedules/{id}", auth.RequireRole("developer", s.deleteSchedule))
	root.Handle("/v1/", s.Auth(s.instrument(public)))
	root.HandleFunc("POST /internal/v1/tasks/{id}/lease", auth.Internal(s.InternalToken, s.lease))
	root.HandleFunc("POST /internal/v1/tasks/{id}/start", auth.Internal(s.InternalToken, s.start))
	root.HandleFunc("POST /internal/v1/tasks/{id}/heartbeat", auth.Internal(s.InternalToken, s.heartbeat))
	root.HandleFunc("POST /internal/v1/tasks/{id}/complete", auth.Internal(s.InternalToken, s.complete))
	root.HandleFunc("POST /internal/v1/tasks/{id}/fail", auth.Internal(s.InternalToken, s.fail))
	root.HandleFunc("POST /internal/v1/workers/{id}/heartbeat", auth.Internal(s.InternalToken, s.workerHeartbeat))
	root.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	root.HandleFunc("GET /health/ready", s.ready)
	root.Handle("GET /metrics", promhttp.Handler())
	root.HandleFunc("GET /openapi.yaml", serveOpenAPI)
	root.HandleFunc("GET /docs", serveDocs)
	return cors(root)
}

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		s.Requests.WithLabelValues(r.Method, routeLabel(r.URL.Path), strconv.Itoa(rw.status)).Observe(time.Since(start).Seconds())
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }

type workflowRequest struct {
	Name  string                       `json:"name"`
	Tasks map[string]workflow.TaskSpec `json:"tasks"`
}

func (s *Server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var req workflowRequest
	if !decode(w, r, &req) {
		return
	}
	p := auth.PrincipalFrom(r.Context())
	d, err := s.Store.CreateWorkflow(r.Context(), p.TenantID, p.UserID, req.Name, workflow.DAG{Name: req.Name, Tasks: req.Tasks})
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 201, d)
}
func (s *Server) createVersion(w http.ResponseWriter, r *http.Request) {
	var req workflowRequest
	if !decode(w, r, &req) {
		return
	}
	p := auth.PrincipalFrom(r.Context())
	d, err := s.Store.CreateVersion(r.Context(), p.TenantID, p.UserID, r.PathValue("id"), workflow.DAG{Tasks: req.Tasks})
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 201, d)
}
func (s *Server) listWorkflows(w http.ResponseWriter, r *http.Request) {
	d, err := s.Store.ListWorkflows(r.Context(), auth.PrincipalFrom(r.Context()).TenantID)
	if handleErr(w, err) {
		return
	}
	if d == nil {
		d = []workflow.Definition{}
	}
	writeJSON(w, 200, map[string]any{"items": d})
}
func (s *Server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	d, err := s.Store.GetWorkflow(r.Context(), auth.PrincipalFrom(r.Context()).TenantID, r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, d)
}
func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 200 {
		writeError(w, 400, "a valid Idempotency-Key header is required")
		return
	}
	var req struct {
		Input            json.RawMessage `json:"input"`
		InputArtifactURI string          `json:"input_artifact_uri,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if len(req.Input) == 0 {
		req.Input = json.RawMessage(`{}`)
	}
	var inputObject map[string]any
	if err := json.Unmarshal(req.Input, &inputObject); err != nil || inputObject == nil {
		writeError(w, 422, "input must be a JSON object")
		return
	}
	p := auth.PrincipalFrom(r.Context())
	run, created, err := s.Store.CreateRun(r.Context(), p.TenantID, p.UserID, r.PathValue("id"), key, req.Input)
	if handleErr(w, err) {
		return
	}
	status := 200
	if created {
		status = 201
	}
	w.Header().Set("Location", "/v1/runs/"+run.ID)
	writeJSON(w, status, run)
}
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.Store.GetRun(r.Context(), auth.PrincipalFrom(r.Context()).TenantID, r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, run)
}
func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	items, err := s.Store.ListRuns(r.Context(), auth.PrincipalFrom(r.Context()).TenantID, 50)
	if handleErr(w, err) {
		return
	}
	if items == nil {
		items = []workflow.Run{}
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	p := auth.PrincipalFrom(r.Context())
	if handleErr(w, s.Store.CancelRun(r.Context(), p.TenantID, p.UserID, r.PathValue("id"))) {
		return
	}
	w.WriteHeader(204)
}
func (s *Server) retryRun(w http.ResponseWriter, r *http.Request) {
	p := auth.PrincipalFrom(r.Context())
	if handleErr(w, s.Store.RetryRun(r.Context(), p.TenantID, p.UserID, r.PathValue("id"))) {
		return
	}
	writeJSON(w, 202, map[string]string{"status": "RUNNING"})
}
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	events, err := s.Store.Events(r.Context(), auth.PrincipalFrom(r.Context()).TenantID, r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	if events == nil {
		events = []workflow.Event{}
	}
	writeJSON(w, 200, map[string]any{"items": events})
}
func (s *Server) workers(w http.ResponseWriter, r *http.Request) {
	items, err := s.Store.ListWorkers(r.Context())
	if handleErr(w, err) {
		return
	}
	if items == nil {
		items = []storage.WorkerInfo{}
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s *Server) deadLetter(w http.ResponseWriter, r *http.Request) {
	items, err := s.Store.DeadLetter(r.Context(), auth.PrincipalFrom(r.Context()).TenantID)
	if handleErr(w, err) {
		return
	}
	if items == nil {
		items = []workflow.TaskRun{}
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	if handleErr(w, s.Store.ReplayDead(r.Context(), auth.PrincipalFrom(r.Context()).TenantID, r.PathValue("id"))) {
		return
	}
	writeJSON(w, 202, map[string]string{"status": "READY"})
}

type workerRequest struct {
	WorkerID string `json:"worker_id"`
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var req workerRequest
	if !decode(w, r, &req) || req.WorkerID == "" {
		return
	}
	t, err := s.Store.LeaseTask(r.Context(), r.PathValue("id"), req.WorkerID, s.LeaseDuration)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, t)
}
func (s *Server) start(w http.ResponseWriter, r *http.Request) {
	var req workerRequest
	if !decode(w, r, &req) {
		return
	}
	t, err := s.Store.StartTask(r.Context(), r.PathValue("id"), req.WorkerID)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, t)
}
func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var req workerRequest
	if !decode(w, r, &req) {
		return
	}
	expires, cancelled, err := s.Store.HeartbeatTask(r.Context(), r.PathValue("id"), req.WorkerID, s.LeaseDuration)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, map[string]any{"lease_expires_at": expires, "cancelled": cancelled})
}
func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID          string          `json:"worker_id"`
		Output            json.RawMessage `json:"output"`
		OutputArtifactURI string          `json:"output_artifact_uri"`
	}
	if !decode(w, r, &req) {
		return
	}
	t, err := s.Store.CompleteTask(r.Context(), r.PathValue("id"), req.WorkerID, req.Output, req.OutputArtifactURI)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, t)
}
func (s *Server) fail(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
		storage.Failure
	}
	if !decode(w, r, &req) {
		return
	}
	t, err := s.Store.FailTask(r.Context(), r.PathValue("id"), req.WorkerID, req.Failure)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, t)
}
func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Handlers    []string        `json:"handlers"`
		ActiveTasks int             `json:"active_tasks"`
		Metadata    json.RawMessage `json:"metadata"`
	}
	if !decode(w, r, &req) {
		return
	}
	if handleErr(w, s.Store.WorkerHeartbeat(r.Context(), r.PathValue("id"), req.Handlers, req.ActiveTasks, req.Metadata)) {
		return
	}
	w.WriteHeader(204)
}

func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkflowDefinitionID string    `json:"workflow_definition_id"`
		CronExpression       string    `json:"cron_expression"`
		Timezone             string    `json:"timezone"`
		MisfirePolicy        string    `json:"misfire_policy"`
		NextExecutionAt      time.Time `json:"next_execution_at"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if req.MisfirePolicy == "" {
		req.MisfirePolicy = "skip"
	}
	if req.MisfirePolicy != "skip" && req.MisfirePolicy != "catch_up_once" {
		writeError(w, 422, "misfire_policy must be skip or catch_up_once")
		return
	}
	location, err := time.LoadLocation(req.Timezone)
	if err != nil {
		writeError(w, 422, "invalid timezone")
		return
	}
	parsed, err := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(req.CronExpression)
	if err != nil {
		writeError(w, 422, "invalid cron expression: "+err.Error())
		return
	}
	if req.NextExecutionAt.IsZero() {
		req.NextExecutionAt = parsed.Next(time.Now().In(location)).UTC()
	}
	p := auth.PrincipalFrom(r.Context())
	var id string
	err = s.Store.Pool.QueryRow(r.Context(), `INSERT INTO schedules(tenant_id,workflow_definition_id,cron_expression,timezone,next_execution_at,misfire_policy) SELECT $1,id,$3,$4,$5,$6 FROM workflow_definitions WHERE id=$2 AND tenant_id=$1 RETURNING id`, p.TenantID, req.WorkflowDefinitionID, req.CronExpression, req.Timezone, req.NextExecutionAt, req.MisfirePolicy).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		err = storage.ErrNotFound
	}
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 201, map[string]any{"id": id, "enabled": true})
}
func (s *Server) patchSchedule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled         *bool      `json:"enabled"`
		NextExecutionAt *time.Time `json:"next_execution_at"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := auth.PrincipalFrom(r.Context())
	ct, err := s.Store.Pool.Exec(r.Context(), `UPDATE schedules SET enabled=COALESCE($3,enabled),next_execution_at=COALESCE($4,next_execution_at) WHERE id=$1 AND tenant_id=$2`, r.PathValue("id"), p.TenantID, req.Enabled, req.NextExecutionAt)
	if err == nil && ct.RowsAffected() == 0 {
		err = storage.ErrNotFound
	}
	if handleErr(w, err) {
		return
	}
	w.WriteHeader(204)
}
func (s *Server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	ct, err := s.Store.Pool.Exec(r.Context(), `DELETE FROM schedules WHERE id=$1 AND tenant_id=$2`, r.PathValue("id"), auth.PrincipalFrom(r.Context()).TenantID)
	if err == nil && ct.RowsAffected() == 0 {
		err = storage.ErrNotFound
	}
	if handleErr(w, err) {
		return
	}
	w.WriteHeader(204)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := s.Store.Ping(ctx); err != nil {
		writeError(w, 503, "database unavailable")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, 400, "request body must contain one JSON value")
		return false
	}
	return true
}
func handleErr(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, storage.ErrNotFound):
		writeError(w, 404, "resource not found")
	case errors.Is(err, storage.ErrConflict):
		writeError(w, 409, "operation conflicts with current state")
	case errors.Is(err, storage.ErrLeaseLost):
		writeError(w, 409, "worker no longer owns this task")
	default:
		var syntax *json.SyntaxError
		if errors.As(err, &syntax) {
			writeError(w, 400, err.Error())
		} else if strings.Contains(err.Error(), "workflow ") || strings.Contains(err.Error(), "task ") || strings.Contains(err.Error(), "required") {
			writeError(w, 422, err.Error())
		} else {
			writeError(w, 500, "internal server error")
		}
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func routeLabel(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := range parts {
		if len(parts[i]) == 36 {
			parts[i] = "{id}"
		}
	}
	return "/" + strings.Join(parts, "/")
}
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "http://localhost:3000" || origin == "http://localhost:5173" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key, Authorization")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func serveOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(openapispec.Spec)
}
func serveDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprint(w, `<!doctype html><html><head><title>RunMesh API</title><script src="https://unpkg.com/@stoplight/elements/web-components.min.js"></script><link rel="stylesheet" href="https://unpkg.com/@stoplight/elements/styles.min.css"></head><body><elements-api apiDescriptionUrl="/openapi.yaml" router="hash" layout="responsive"></elements-api></body></html>`)
}
