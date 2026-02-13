package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"GoTTDust/internal/common"
	"GoTTDust/internal/health"
	pipelinemgr "GoTTDust/internal/pipeline"
	"GoTTDust/internal/schema"
	streammgr "GoTTDust/internal/stream"
)

// APIError is the standard error response format.
type APIError struct {
	Error APIErrorBody `json:"error"`
}

// APIErrorBody holds error details.
type APIErrorBody struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
	TraceID string      `json:"trace_id,omitempty"`
}

// EventCallback is called when a domain event occurs in the control plane.
type EventCallback func(eventType, resourceType, resourceID, actor string)

// APIHandler holds all control plane API dependencies.
type APIHandler struct {
	schemaStore     *schema.Store
	streamManager   *streammgr.Manager
	pipelineManager *pipelinemgr.Manager
	healthTracker   *health.Tracker
	queryHandler    *QueryRESTHandler
	onEvent         EventCallback
}

// NewAPIHandler creates a new API handler.
func NewAPIHandler(
	schemaStore *schema.Store,
	streamManager *streammgr.Manager,
	pipelineManager *pipelinemgr.Manager,
	healthTracker *health.Tracker,
) *APIHandler {
	return &APIHandler{
		schemaStore:     schemaStore,
		streamManager:   streamManager,
		pipelineManager: pipelineManager,
		healthTracker:   healthTracker,
	}
}

// SetQueryHandler sets the REST query handler for routing /streams/{id}/records.
func (h *APIHandler) SetQueryHandler(qh *QueryRESTHandler) {
	h.queryHandler = qh
}

// SetEventCallback sets the callback for domain events.
func (h *APIHandler) SetEventCallback(fn EventCallback) {
	h.onEvent = fn
}

func (h *APIHandler) publishEvent(eventType, resourceType, resourceID, actor string) {
	if h.onEvent != nil {
		h.onEvent(eventType, resourceType, resourceID, actor)
	}
}

// RegisterRoutes registers all REST API routes on the given mux.
func (h *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	// Schema endpoints
	mux.HandleFunc("/api/v1/schemas", h.handleSchemas)
	mux.HandleFunc("/api/v1/schemas/", h.handleSchemaByID)

	// Stream endpoints
	mux.HandleFunc("/api/v1/streams", h.handleStreams)
	mux.HandleFunc("/api/v1/streams/", h.handleStreamByID)

	// Pipeline endpoints
	mux.HandleFunc("/api/v1/pipelines", h.handlePipelines)
	mux.HandleFunc("/api/v1/pipelines/", h.handlePipelineByID)
}

// --- Schema handlers ---

func (h *APIHandler) handleSchemas(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createSchema(w, r)
	case http.MethodGet:
		h.listSchemas(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

func (h *APIHandler) createSchema(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string          `json:"name"`
		Namespace   string          `json:"namespace"`
		Description string          `json:"description"`
		Definition  json.RawMessage `json:"definition"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "name is required")
		return
	}
	if req.Namespace == "" {
		req.Namespace = "default"
	}
	if len(req.Definition) == 0 {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "definition is required")
		return
	}
	if len(req.Definition) > 1048576 {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "definition must be ≤ 1MB")
		return
	}

	ctx := r.Context()
	sc, err := h.schemaStore.CreateSchema(ctx, req.Name, req.Namespace, req.Description, "api")
	if err != nil {
		handleDBError(w, err)
		return
	}

	sv, err := h.schemaStore.CreateVersion(ctx, sc.SchemaID, req.Definition, common.CompatBackward, "api")
	if err != nil {
		handleDBError(w, err)
		return
	}

	h.publishEvent("SchemaCreated", "schema", sc.SchemaID, "api")

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"schema_id":   sc.SchemaID,
		"name":        sc.Name,
		"namespace":   sc.Namespace,
		"version":     sv.Version,
		"fingerprint": sv.Fingerprint,
	})
}

func (h *APIHandler) listSchemas(w http.ResponseWriter, r *http.Request) {
	schemas, err := h.schemaStore.ListSchemas(r.Context())
	if err != nil {
		handleDBError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, schemas)
}

func (h *APIHandler) handleSchemaByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/schemas/")
	parts := strings.Split(path, "/")
	schemaID := parts[0]

	if len(parts) >= 2 && parts[1] == "versions" {
		switch r.Method {
		case http.MethodPost:
			h.addSchemaVersion(w, r, schemaID)
		case http.MethodGet:
			h.listSchemaVersions(w, r, schemaID)
		default:
			writeError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
		}
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getSchema(w, r, schemaID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

func (h *APIHandler) getSchema(w http.ResponseWriter, r *http.Request, schemaID string) {
	sc, err := h.schemaStore.GetSchema(r.Context(), schemaID)
	if err != nil {
		handleDBError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

func (h *APIHandler) addSchemaVersion(w http.ResponseWriter, r *http.Request, schemaID string) {
	var req struct {
		Definition        json.RawMessage `json:"definition"`
		CompatibilityMode string          `json:"compatibility_mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	if req.CompatibilityMode == "" {
		req.CompatibilityMode = common.CompatBackward
	}

	sv, err := h.schemaStore.CreateVersion(r.Context(), schemaID, req.Definition, req.CompatibilityMode, "api")
	if err != nil {
		handleDBError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, sv)
}

func (h *APIHandler) listSchemaVersions(w http.ResponseWriter, r *http.Request, schemaID string) {
	versions, err := h.schemaStore.ListVersions(r.Context(), schemaID)
	if err != nil {
		handleDBError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, versions)
}

// --- Stream handlers ---

func (h *APIHandler) handleStreams(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createStream(w, r)
	case http.MethodGet:
		h.listStreams(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

func (h *APIHandler) createStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name            string                  `json:"name"`
		Description     string                  `json:"description"`
		SchemaID        string                  `json:"schema_id"`
		RetentionPolicy *common.RetentionPolicy `json:"retention_policy"`
		PartitionConfig *common.PartitionConfig `json:"partition_config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	if req.Name == "" || req.SchemaID == "" {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "name and schema_id are required")
		return
	}

	stream, err := h.streamManager.CreateStream(r.Context(), req.Name, req.Description, req.SchemaID, "api", req.RetentionPolicy, req.PartitionConfig)
	if err != nil {
		handleDBError(w, err)
		return
	}

	h.publishEvent("StreamCreated", "stream", stream.StreamID, "api")

	writeJSON(w, http.StatusCreated, stream)
}

func (h *APIHandler) listStreams(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	streams, err := h.streamManager.ListStreams(r.Context(), status)
	if err != nil {
		handleDBError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, streams)
}

// HandleStreamByID is the exported handler for stream-by-ID requests.
func (h *APIHandler) HandleStreamByID(w http.ResponseWriter, r *http.Request) {
	h.handleStreamByID(w, r)
}

func (h *APIHandler) handleStreamByID(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/v1/streams/")
	parts := strings.SplitN(trimmed, "/", 2)
	streamID := parts[0]

	// Route /streams/{id}/records to query handler
	if len(parts) > 1 && strings.HasPrefix(parts[1], "records") && h.queryHandler != nil {
		h.queryHandler.HandleStreamRecords(w, r, streamID)
		return
	}

	switch r.Method {
	case http.MethodGet:
		stream, err := h.streamManager.GetStream(r.Context(), streamID)
		if err != nil {
			handleDBError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stream)

	case http.MethodPatch:
		var req struct {
			Description     *string                 `json:"description"`
			RetentionPolicy *common.RetentionPolicy `json:"retention_policy"`
			PartitionConfig *common.PartitionConfig `json:"partition_config"`
			Status          *string                 `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
			return
		}

		if req.Status != nil {
			if err := h.streamManager.TransitionState(r.Context(), streamID, *req.Status); err != nil {
				handleDBError(w, err)
				return
			}
		}

		stream, err := h.streamManager.UpdateStream(r.Context(), streamID, req.Description, req.RetentionPolicy, req.PartitionConfig)
		if err != nil {
			handleDBError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stream)

	case http.MethodDelete:
		if err := h.streamManager.DeleteStream(r.Context(), streamID); err != nil {
			handleDBError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"success": true})

	default:
		writeError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

// --- Pipeline handlers ---

func (h *APIHandler) handlePipelines(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createPipeline(w, r)
	case http.MethodGet:
		h.listPipelines(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

func (h *APIHandler) createPipeline(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		StreamID string `json:"stream_id"`
		Stages   []struct {
			Type    string          `json:"type"`
			Config  json.RawMessage `json:"config"`
			Enabled *bool           `json:"enabled"`
		} `json:"stages"`
		DLQConfig *struct {
			Enabled    bool   `json:"enabled"`
			StreamName string `json:"stream_name"`
		} `json:"dlq_config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	if req.Name == "" || req.StreamID == "" {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "name and stream_id are required")
		return
	}

	stages := make([]pipelinemgr.StageInput, len(req.Stages))
	for i, s := range req.Stages {
		enabled := true
		if s.Enabled != nil {
			enabled = *s.Enabled
		}
		stages[i] = pipelinemgr.StageInput{
			StageType: s.Type,
			Config:    s.Config,
			Enabled:   enabled,
		}
	}

	pipeline, pipelineStages, err := h.pipelineManager.CreatePipeline(r.Context(), pipelinemgr.CreatePipelineRequest{
		Name:     req.Name,
		StreamID: req.StreamID,
		Stages:   stages,
	})
	if err != nil {
		handleDBError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"pipeline": pipeline,
		"stages":   pipelineStages,
	})
}

func (h *APIHandler) listPipelines(w http.ResponseWriter, r *http.Request) {
	streamID := r.URL.Query().Get("stream_id")
	pipelines, err := h.pipelineManager.ListPipelines(r.Context(), streamID)
	if err != nil {
		handleDBError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pipelines)
}

func (h *APIHandler) handlePipelineByID(w http.ResponseWriter, r *http.Request) {
	pipelineID := strings.TrimPrefix(r.URL.Path, "/api/v1/pipelines/")
	pipelineID = strings.Split(pipelineID, "/")[0]

	switch r.Method {
	case http.MethodGet:
		pipeline, err := h.pipelineManager.GetPipeline(r.Context(), pipelineID)
		if err != nil {
			handleDBError(w, err)
			return
		}
		stages, err := h.pipelineManager.GetPipelineStages(r.Context(), pipelineID)
		if err != nil {
			handleDBError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"pipeline": pipeline,
			"stages":   stages,
		})

	case http.MethodPatch:
		var req struct {
			Stages []struct {
				Type    string          `json:"type"`
				Config  json.RawMessage `json:"config"`
				Enabled bool            `json:"enabled"`
			} `json:"stages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
			return
		}

		stages := make([]pipelinemgr.StageInput, len(req.Stages))
		for i, s := range req.Stages {
			stages[i] = pipelinemgr.StageInput{
				StageType: s.Type,
				Config:    s.Config,
				Enabled:   s.Enabled,
			}
		}

		updatedStages, err := h.pipelineManager.UpdatePipelineStages(r.Context(), pipelineID, stages)
		if err != nil {
			handleDBError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, updatedStages)

	case http.MethodDelete:
		if err := h.pipelineManager.DeletePipeline(r.Context(), pipelineID); err != nil {
			handleDBError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"success": true})

	default:
		writeError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(APIError{
		Error: APIErrorBody{
			Code:    code,
			Message: message,
			TraceID: common.GenerateTraceID(),
		},
	})
}

func handleDBError(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		writeError(w, http.StatusNotFound, "NOT_FOUND", msg)
	case strings.Contains(msg, "validation error") || strings.Contains(msg, "invalid"):
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", msg)
	case strings.Contains(msg, "conflict") || strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique"):
		writeError(w, http.StatusConflict, "CONFLICT", msg)
	case strings.Contains(msg, "invalid state") || strings.Contains(msg, "cannot transition"):
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", msg)
	default:
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", fmt.Sprintf("internal error: %v", err))
	}
}
