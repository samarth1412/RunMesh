package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron/v3"
	"github.com/runmesh/runmesh/internal/artifact"
	"github.com/runmesh/runmesh/internal/auth"
	"github.com/runmesh/runmesh/internal/live"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/telemetry"
	"github.com/runmesh/runmesh/internal/workflow"
	openapispec "github.com/runmesh/runmesh/openapi"
)

const maxBodyBytes = 1 << 20

type Server struct {
	Store           *storage.Store
	LeaseDuration   time.Duration
	Auth            func(http.Handler) http.Handler
	WorkerAuth      func(http.Handler) http.Handler
	RateLimit       func(http.Handler) http.Handler
	DependencyReady func(context.Context) error
	APIKeyPepper    string
	Artifacts       *artifact.Manager
	Live            *live.Broker
	Requests        *prometheus.HistogramVec
}

func New(store *storage.Store, lease time.Duration, authMiddleware, workerAuth, rateLimit func(http.Handler) http.Handler, dependencyReady func(context.Context) error, apiKeyPepper string, artifacts ...*artifact.Manager) *Server {
	req := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "api_request_duration_seconds", Help: "Public API request latency", Buckets: prometheus.DefBuckets}, []string{"method", "route", "status"})
	prometheus.MustRegister(req)
	server := &Server{Store: store, LeaseDuration: lease, Auth: authMiddleware, WorkerAuth: workerAuth, RateLimit: rateLimit, DependencyReady: dependencyReady, APIKeyPepper: apiKeyPepper, Requests: req}
	if len(artifacts) > 0 {
		server.Artifacts = artifacts[0]
	}
	return server
}

func (s *Server) Handler() http.Handler {
	root := http.NewServeMux()
	public := http.NewServeMux()
	public.HandleFunc("POST /v1/workflows", auth.Require("developer", "workflows:write", s.createWorkflow))
	public.HandleFunc("GET /v1/workflows", auth.Require("viewer", "workflows:read", s.listWorkflows))
	public.HandleFunc("GET /v1/workflows/{id}", auth.Require("viewer", "workflows:read", s.getWorkflow))
	public.HandleFunc("POST /v1/workflows/{id}/versions", auth.Require("developer", "workflows:write", s.createVersion))
	public.HandleFunc("POST /v1/workflows/{id}/runs", auth.Require("operator", "runs:execute", s.createRun))
	public.HandleFunc("GET /v1/runs", auth.Require("viewer", "runs:read", s.listRuns))
	public.HandleFunc("GET /v1/runs/{id}", auth.Require("viewer", "runs:read", s.getRun))
	public.HandleFunc("POST /v1/runs/{id}/cancel", auth.Require("operator", "runs:execute", s.cancelRun))
	public.HandleFunc("POST /v1/runs/{id}/retry", auth.Require("operator", "runs:execute", s.retryRun))
	public.HandleFunc("GET /v1/runs/{id}/events", auth.Require("viewer", "runs:read", s.events))
	public.HandleFunc("GET /v1/workers", auth.Require("viewer", "workers:read", s.workers))
	public.HandleFunc("GET /v1/dead-letter", auth.Require("operator", "dead-letter:read", s.deadLetter))
	public.HandleFunc("POST /v1/dead-letter/{id}/replay", auth.Require("operator", "dead-letter:replay", s.replay))
	public.HandleFunc("POST /v1/schedules", auth.Require("developer", "schedules:write", s.createSchedule))
	public.HandleFunc("PATCH /v1/schedules/{id}", auth.Require("developer", "schedules:write", s.patchSchedule))
	public.HandleFunc("DELETE /v1/schedules/{id}", auth.Require("developer", "schedules:write", s.deleteSchedule))
	public.HandleFunc("GET /v1/api-keys", auth.RequireHumanRole("admin", s.listAPIKeys))
	public.HandleFunc("POST /v1/api-keys", auth.RequireHumanRole("admin", s.createAPIKey))
	public.HandleFunc("POST /v1/api-keys/{id}/rotate", auth.RequireHumanRole("admin", s.rotateAPIKey))
	public.HandleFunc("DELETE /v1/api-keys/{id}", auth.RequireHumanRole("admin", s.revokeAPIKey))
	public.HandleFunc("POST /v1/artifacts/uploads", auth.Require("developer", "artifacts:write", s.createArtifactUpload))
	public.HandleFunc("POST /v1/artifacts/{id}/complete", auth.Require("developer", "artifacts:write", s.completeArtifactUpload))
	public.HandleFunc("GET /v1/artifacts/{id}/download", auth.Require("viewer", "artifacts:read", s.downloadArtifact))
	public.HandleFunc("GET /v1/stream", auth.Require("viewer", "runs:read", s.stream))
	publicHandler := s.instrument(public)
	if s.RateLimit != nil {
		publicHandler = s.RateLimit(publicHandler)
	}
	root.Handle("/v1/", s.Auth(publicHandler))
	internal := http.NewServeMux()
	internal.HandleFunc("POST /internal/v1/tasks/{id}/lease", s.lease)
	internal.HandleFunc("POST /internal/v1/tasks/{id}/start", s.start)
	internal.HandleFunc("POST /internal/v1/tasks/{id}/heartbeat", s.heartbeat)
	internal.HandleFunc("POST /internal/v1/tasks/{id}/complete", s.complete)
	internal.HandleFunc("POST /internal/v1/tasks/{id}/fail", s.fail)
	internal.HandleFunc("POST /internal/v1/workers/{id}/heartbeat", s.workerHeartbeat)
	internal.HandleFunc("POST /internal/v1/artifacts/uploads", s.createArtifactUpload)
	internal.HandleFunc("POST /internal/v1/artifacts/{id}/complete", s.completeArtifactUpload)
	internal.HandleFunc("GET /internal/v1/artifacts/{id}/download", s.downloadArtifact)
	root.Handle("/internal/v1/", s.WorkerAuth(internal))
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
		duration := time.Since(start)
		route := routeLabel(r.URL.Path)
		s.Requests.WithLabelValues(r.Method, route, strconv.Itoa(rw.status)).Observe(duration.Seconds())
		principal := auth.PrincipalFrom(r.Context())
		telemetry.Logger(r.Context(), "tenant_id", principal.TenantID).Info("http request", "method", r.Method, "route", route, "status", rw.status, "duration_ms", duration.Milliseconds())
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	w.status = http.StatusSwitchingProtocols
	return hijacker.Hijack()
}

func (w *responseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

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
		Input           json.RawMessage `json:"input"`
		InputArtifactID string          `json:"input_artifact_id,omitempty"`
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
	run, created, err := s.Store.CreateRun(r.Context(), p.TenantID, p.UserID, r.PathValue("id"), key, req.Input, req.InputArtifactID)
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

func (s *Server) createArtifactUpload(w http.ResponseWriter, r *http.Request) {
	if s.Artifacts == nil {
		writeError(w, http.StatusServiceUnavailable, "artifact storage is not configured")
		return
	}
	var req struct {
		Kind           string  `json:"kind"`
		ContentType    string  `json:"content_type"`
		SizeBytes      int64   `json:"size_bytes"`
		ChecksumSHA256 string  `json:"checksum_sha256,omitempty"`
		TaskRunID      *string `json:"task_run_id,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := auth.PrincipalFrom(r.Context())
	var upload artifact.Upload
	var err error
	if strings.HasPrefix(r.URL.Path, "/internal/") {
		upload, err = s.Artifacts.CreateInternalUpload(r.Context(), p.TenantID, p.UserID, req.Kind, req.ContentType, req.SizeBytes, req.ChecksumSHA256, req.TaskRunID)
	} else {
		upload, err = s.Artifacts.CreateUpload(r.Context(), p.TenantID, p.UserID, req.Kind, req.ContentType, req.SizeBytes, req.ChecksumSHA256, req.TaskRunID)
	}
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, 404, "task not found")
			return
		}
		writeError(w, 422, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, upload)
}

func (s *Server) completeArtifactUpload(w http.ResponseWriter, r *http.Request) {
	if s.Artifacts == nil {
		writeError(w, http.StatusServiceUnavailable, "artifact storage is not configured")
		return
	}
	p := auth.PrincipalFrom(r.Context())
	a, err := s.Artifacts.Complete(r.Context(), p.TenantID, r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) downloadArtifact(w http.ResponseWriter, r *http.Request) {
	if s.Artifacts == nil {
		writeError(w, http.StatusServiceUnavailable, "artifact storage is not configured")
		return
	}
	p := auth.PrincipalFrom(r.Context())
	var download artifact.Download
	var err error
	if strings.HasPrefix(r.URL.Path, "/internal/") {
		download, err = s.Artifacts.DownloadInternal(r.Context(), p.TenantID, r.PathValue("id"))
	} else {
		download, err = s.Artifacts.Download(r.Context(), p.TenantID, r.PathValue("id"))
	}
	if handleErr(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, download)
}
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	tenantID := auth.PrincipalFrom(r.Context()).TenantID
	run, err := s.Store.GetRun(r.Context(), tenantID, r.PathValue("id"))
	if handleErr(w, err) {
		return
	}
	if s.Artifacts != nil {
		if run.InputArtifactURI != nil {
			if signed, signErr := s.Artifacts.Download(r.Context(), tenantID, *run.InputArtifactURI); signErr == nil {
				run.InputArtifactDownloadURL = signed.URL
			}
		}
		for index := range run.Tasks {
			if run.Tasks[index].OutputArtifactURI != nil {
				if signed, signErr := s.Artifacts.Download(r.Context(), tenantID, *run.Tasks[index].OutputArtifactURI); signErr == nil {
					run.Tasks[index].OutputArtifactDownloadURL = signed.URL
				}
			}
			if run.Tasks[index].LogArtifactURI != nil {
				if signed, signErr := s.Artifacts.Download(r.Context(), tenantID, *run.Tasks[index].LogArtifactURI); signErr == nil {
					run.Tasks[index].LogArtifactDownloadURL = signed.URL
				}
			}
		}
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
	items, err := s.Store.ListWorkers(r.Context(), auth.PrincipalFrom(r.Context()).TenantID)
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
	p := auth.PrincipalFrom(r.Context())
	if handleErr(w, s.Store.ReplayDead(r.Context(), p.TenantID, p.UserID, r.PathValue("id"))) {
		return
	}
	writeJSON(w, 202, map[string]string{"status": "READY"})
}

var allowedAPIKeyScopes = map[string]bool{
	"workflows:read": true, "workflows:write": true,
	"runs:read": true, "runs:execute": true,
	"schedules:write": true, "workers:read": true, "workers:execute": true,
	"dead-letter:read": true, "dead-letter:replay": true,
	"artifacts:read": true, "artifacts:write": true,
}

func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.Store.ListAPIKeys(r.Context(), auth.PrincipalFrom(r.Context()).TenantID)
	if handleErr(w, err) {
		return
	}
	if keys == nil {
		keys = []storage.APIKey{}
	}
	writeJSON(w, 200, map[string]any{"items": keys})
}

func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Name      string     `json:"name"`
		Scopes    []string   `json:"scopes"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if !decode(w, r, &request) {
		return
	}
	request.Name = strings.TrimSpace(request.Name)
	if request.Name == "" || len(request.Name) > 100 || len(request.Scopes) == 0 {
		writeError(w, 422, "name and at least one scope are required")
		return
	}
	seen := map[string]bool{}
	for _, scope := range request.Scopes {
		if !allowedAPIKeyScopes[scope] || seen[scope] {
			writeError(w, 422, "invalid or duplicate API key scope")
			return
		}
		seen[scope] = true
	}
	if request.ExpiresAt != nil && !request.ExpiresAt.After(time.Now()) {
		writeError(w, 422, "expires_at must be in the future")
		return
	}
	id, token, hash, err := auth.GenerateAPIKey(s.APIKeyPepper)
	if handleErr(w, err) {
		return
	}
	principal := auth.PrincipalFrom(r.Context())
	key, err := s.Store.CreateAPIKey(r.Context(), id, principal.TenantID, principal.UserID, request.Name, request.Scopes, request.ExpiresAt, hash)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 201, map[string]any{"api_key": key, "token": token})
}

func (s *Server) rotateAPIKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, 400, "invalid API key id")
		return
	}
	token, hash, err := auth.GenerateAPIKeyForID(id, s.APIKeyPepper)
	if handleErr(w, err) {
		return
	}
	principal := auth.PrincipalFrom(r.Context())
	key, err := s.Store.RotateAPIKey(r.Context(), id, principal.TenantID, principal.UserID, hash)
	if handleErr(w, err) {
		return
	}
	writeJSON(w, 200, map[string]any{"api_key": key, "token": token})
}

func (s *Server) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	principal := auth.PrincipalFrom(r.Context())
	if handleErr(w, s.Store.RevokeAPIKey(r.Context(), r.PathValue("id"), principal.TenantID, principal.UserID)) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type workerRequest struct {
	WorkerID string `json:"worker_id"`
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var req workerRequest
	if !decode(w, r, &req) || req.WorkerID == "" {
		return
	}
	if !s.authorizeTask(w, r) {
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
	if !s.authorizeTask(w, r) {
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
	if !s.authorizeTask(w, r) {
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
	if !s.authorizeTask(w, r) {
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
	if !s.authorizeTask(w, r) {
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
	if handleErr(w, s.Store.WorkerHeartbeat(r.Context(), auth.PrincipalFrom(r.Context()).TenantID, r.PathValue("id"), req.Handlers, req.ActiveTasks, req.Metadata)) {
		return
	}
	w.WriteHeader(204)
}

func (s *Server) authorizeTask(w http.ResponseWriter, r *http.Request) bool {
	err := s.Store.TaskBelongsToTenant(r.Context(), r.PathValue("id"), auth.PrincipalFrom(r.Context()).TenantID)
	if handleErr(w, err) {
		return false
	}
	return true
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
	if s.DependencyReady != nil {
		if err := s.DependencyReady(ctx); err != nil {
			writeError(w, 503, "rate limiter unavailable")
			return
		}
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
