package rest

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"GoTTDust/internal/common"
	"GoTTDust/internal/query"
	"GoTTDust/internal/storage"
)

// QueryRESTHandler handles REST query endpoints.
type QueryRESTHandler struct {
	planner        *query.Planner
	storageManager *storage.Manager
}

// NewQueryRESTHandler creates a new REST query handler.
func NewQueryRESTHandler(planner *query.Planner, storageManager *storage.Manager) *QueryRESTHandler {
	return &QueryRESTHandler{
		planner:        planner,
		storageManager: storageManager,
	}
}

// RegisterRoutes registers query REST routes.
func (qh *QueryRESTHandler) RegisterRoutes(mux *http.ServeMux) {
	// These are handled via the stream path prefix in the main API handler
}

// HandleStreamRecords handles GET /api/v1/streams/{id}/records
func (qh *QueryRESTHandler) HandleStreamRecords(w http.ResponseWriter, r *http.Request, streamID string) {
	if r.Method != http.MethodGet {
		writeQueryError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
		return
	}

	q := r.URL.Query()

	// Check for key lookup
	keyField := q.Get("key_field")
	keyValue := q.Get("key_value")
	if keyField != "" && keyValue != "" {
		qh.handleKeyLookup(w, r, streamID, keyField, keyValue)
		return
	}

	// Time-range query
	qh.handleTimeRangeQuery(w, r, streamID)
}

func (qh *QueryRESTHandler) handleTimeRangeQuery(w http.ResponseWriter, r *http.Request, streamID string) {
	q := r.URL.Query()

	// Parse start time
	startStr := q.Get("start")
	if startStr == "" {
		writeQueryError(w, http.StatusBadRequest, "VALIDATION_ERROR", "start parameter is required")
		return
	}
	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		writeQueryError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid start time format, use RFC3339")
		return
	}

	// Parse end time
	endStr := q.Get("end")
	if endStr == "" {
		writeQueryError(w, http.StatusBadRequest, "VALIDATION_ERROR", "end parameter is required")
		return
	}
	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		writeQueryError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid end time format, use RFC3339")
		return
	}

	// Validate time range <= 7 days
	if endTime.Sub(startTime) > 7*24*time.Hour {
		writeQueryError(w, http.StatusBadRequest, "VALIDATION_ERROR", "time range must be <= 7 days")
		return
	}

	// Parse limit
	limit := 1000
	if limitStr := q.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			if l > 10000 {
				l = 10000
			}
			if l > 0 {
				limit = l
			}
		}
	}

	cursor := q.Get("cursor")

	// Execute query via planner
	sid := common.StreamID(streamID)
	execStart := time.Now()
	plan, err := qh.planner.PlanTimeRange(sid, startTime, endTime, limit, cursor)
	if err != nil {
		writeQueryError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	// Build response
	type recordResponse struct {
		RecordID       string          `json:"record_id"`
		SequenceNumber int64           `json:"sequence_number"`
		IngestedAt     string          `json:"ingested_at"`
		Payload        json.RawMessage `json:"payload"`
	}

	records := make([]recordResponse, 0)
	var nextCursor string

	// Read records from S3 files identified by the query plan
	for _, file := range plan.Files {
		data, err := qh.storageManager.ReadFile(r.Context(), file.Key)
		if err != nil {
			continue // skip unreadable files
		}
		// Deserialize batch (JSON-encoded ParquetBatch)
		var batch struct {
			Records []storage.ParquetRecord `json:"records"`
		}
		if err := json.Unmarshal(data, &batch); err != nil {
			continue
		}
		for _, rec := range batch.Records {
			if len(records) >= limit {
				nextCursor = rec.RecordID
				break
			}
			records = append(records, recordResponse{
				RecordID:       rec.RecordID,
				SequenceNumber: rec.Sequence,
				IngestedAt:     time.UnixMicro(rec.IngestedAt).Format(time.RFC3339Nano),
				Payload:        rec.Payload,
			})
		}
		if len(records) >= limit {
			break
		}
	}

	execDuration := time.Since(execStart)

	writeQueryJSON(w, http.StatusOK, map[string]interface{}{
		"records":     records,
		"next_cursor": nextCursor,
		"metadata": map[string]interface{}{
			"records_scanned":    len(records),
			"partitions_scanned": len(plan.Partitions),
			"execution_time_ms":  execDuration.Milliseconds(),
		},
	})
}

func (qh *QueryRESTHandler) handleKeyLookup(w http.ResponseWriter, r *http.Request, streamID, keyField, keyValue string) {
	// Key lookup via planner
	sid := common.StreamID(streamID)
	plan, err := qh.planner.PlanKeyLookup(sid, keyField, keyValue)
	if err != nil {
		writeQueryError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	// Read records from S3 files identified by the query plan
	for _, file := range plan.Files {
		data, err := qh.storageManager.ReadFile(r.Context(), file.Key)
		if err != nil {
			continue
		}
		var batch struct {
			Records []storage.ParquetRecord `json:"records"`
		}
		if err := json.Unmarshal(data, &batch); err != nil {
			continue
		}
		for _, rec := range batch.Records {
			if (keyField == "_record_id" && rec.RecordID == keyValue) ||
				(keyField == "_stream_id" && rec.StreamID == keyValue) {
				writeQueryJSON(w, http.StatusOK, map[string]interface{}{
					"found": true,
					"record": map[string]interface{}{
						"record_id":       rec.RecordID,
						"sequence_number": rec.Sequence,
						"ingested_at":     time.UnixMicro(rec.IngestedAt).Format(time.RFC3339Nano),
						"payload":         json.RawMessage(rec.Payload),
					},
				})
				return
			}
		}
	}

	writeQueryJSON(w, http.StatusOK, map[string]interface{}{
		"found":  false,
		"record": nil,
	})
}

func writeQueryJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeQueryError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(APIError{
		Error: APIErrorBody{
			Code:    code,
			Message: message,
			TraceID: common.GenerateTraceID(),
		},
	})
}

// ParseStreamRecordsPath extracts stream ID and sub-path from /api/v1/streams/{id}/records...
func ParseStreamRecordsPath(path string) (streamID string, subPath string, ok bool) {
	trimmed := strings.TrimPrefix(path, "/api/v1/streams/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 1 || parts[0] == "" {
		return "", "", false
	}
	streamID = parts[0]
	if len(parts) > 1 {
		subPath = parts[1]
	}
	return streamID, subPath, true
}
