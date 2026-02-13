package rest

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"GoTTDust/internal/common"
)

// AdminHandler holds dependencies for admin REST endpoints.
type AdminHandler struct {
	db *sql.DB
}

// NewAdminHandler creates a new admin handler.
func NewAdminHandler(db *sql.DB) *AdminHandler {
	return &AdminHandler{db: db}
}

// RegisterRoutes registers admin API routes.
func (ah *AdminHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/config", ah.handleConfig)
	mux.HandleFunc("/api/v1/api-keys", ah.handleAPIKeys)
	mux.HandleFunc("/api/v1/api-keys/", ah.handleAPIKeyByID)
}

// --- System Config ---

func (ah *AdminHandler) handleConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	switch r.Method {
	case http.MethodGet:
		rows, err := ah.db.QueryContext(ctx,
			`SELECT config_key, config_value FROM system_config ORDER BY config_key`)
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		defer rows.Close()

		result := make(map[string]json.RawMessage)
		for rows.Next() {
			var key string
			var value json.RawMessage
			if err := rows.Scan(&key, &value); err != nil {
				continue
			}
			result[key] = value
		}
		writeAdminJSON(w, http.StatusOK, result)

	case http.MethodPatch:
		var updates map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
			writeAdminError(w, http.StatusBadRequest, "INVALID_JSON", "invalid JSON body")
			return
		}

		for key, value := range updates {
			_, err := ah.db.ExecContext(ctx,
				`INSERT INTO system_config (config_key, config_value, updated_by)
				 VALUES ($1, $2, $3)
				 ON CONFLICT (config_key) DO UPDATE SET config_value = $2, updated_at = NOW()`,
				key, value, "api")
			if err != nil {
				writeAdminError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
				return
			}
		}

		writeAdminJSON(w, http.StatusOK, map[string]interface{}{
			"updated_keys": len(updates),
			"updated_at":   time.Now().UTC().Format(time.RFC3339),
		})

	default:
		writeAdminError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

// --- API Key Management ---

func (ah *AdminHandler) handleAPIKeys(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Name              string          `json:"name"`
			Role              string          `json:"role"`
			StreamPermissions json.RawMessage `json:"stream_permissions"`
			ExpiresInDays     *int            `json:"expires_in_days"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAdminError(w, http.StatusBadRequest, "INVALID_JSON", "invalid JSON body")
			return
		}
		if req.Name == "" || req.Role == "" {
			writeAdminError(w, http.StatusBadRequest, "VALIDATION_ERROR", "name and role are required")
			return
		}
		if req.Role != common.RoleAdmin && req.Role != common.RoleWriter && req.Role != common.RoleReader {
			writeAdminError(w, http.StatusBadRequest, "VALIDATION_ERROR", "role must be admin, writer, or reader")
			return
		}
		if req.StreamPermissions == nil {
			req.StreamPermissions = json.RawMessage(`[]`)
		}

		// Generate API key
		rawKey := make([]byte, 32)
		if _, err := rand.Read(rawKey); err != nil {
			writeAdminError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate key")
			return
		}
		apiKey := "ttd_live_" + hex.EncodeToString(rawKey)
		hash := sha256.Sum256([]byte(apiKey))
		keyHash := hex.EncodeToString(hash[:])

		var expiresAt *time.Time
		if req.ExpiresInDays != nil {
			t := time.Now().AddDate(0, 0, *req.ExpiresInDays)
			expiresAt = &t
		}

		var keyID string
		err := ah.db.QueryRowContext(ctx,
			`INSERT INTO api_keys (key_hash, name, role, stream_permissions, expires_at, created_by)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 RETURNING key_id`,
			keyHash, req.Name, req.Role, req.StreamPermissions, expiresAt, "api",
		).Scan(&keyID)
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, "INTERNAL_ERROR", fmt.Sprintf("create key: %v", err))
			return
		}

		writeAdminJSON(w, http.StatusCreated, map[string]interface{}{
			"key_id":     keyID,
			"api_key":    apiKey,
			"name":       req.Name,
			"role":       req.Role,
			"expires_at": expiresAt,
			"created_at": time.Now().UTC().Format(time.RFC3339),
		})

	case http.MethodGet:
		rows, err := ah.db.QueryContext(ctx,
			`SELECT key_id, name, role, stream_permissions, created_at, expires_at, last_used_at
			 FROM api_keys ORDER BY created_at DESC`)
		if err != nil {
			writeAdminError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}
		defer rows.Close()

		type apiKeyResponse struct {
			KeyID             string          `json:"key_id"`
			Name              string          `json:"name"`
			Role              string          `json:"role"`
			StreamPermissions json.RawMessage `json:"stream_permissions"`
			CreatedAt         time.Time       `json:"created_at"`
			ExpiresAt         *time.Time      `json:"expires_at,omitempty"`
			LastUsedAt        *time.Time      `json:"last_used_at,omitempty"`
		}

		var keys []apiKeyResponse
		for rows.Next() {
			var k apiKeyResponse
			if err := rows.Scan(&k.KeyID, &k.Name, &k.Role, &k.StreamPermissions,
				&k.CreatedAt, &k.ExpiresAt, &k.LastUsedAt); err != nil {
				continue
			}
			keys = append(keys, k)
		}

		writeAdminJSON(w, http.StatusOK, map[string]interface{}{
			"api_keys": keys,
		})

	default:
		writeAdminError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
	}
}

func (ah *AdminHandler) handleAPIKeyByID(w http.ResponseWriter, r *http.Request) {
	keyID := strings.TrimPrefix(r.URL.Path, "/api/v1/api-keys/")
	keyID = strings.Split(keyID, "/")[0]

	if r.Method != http.MethodDelete {
		writeAdminError(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
		return
	}

	result, err := ah.db.ExecContext(r.Context(),
		`DELETE FROM api_keys WHERE key_id = $1`, keyID)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		writeAdminError(w, http.StatusNotFound, "NOT_FOUND", "API key not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func writeAdminJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeAdminError(w http.ResponseWriter, status int, code, message string) {
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
