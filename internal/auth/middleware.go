package auth

import (
	"context"
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

// --- Principal (authenticated identity) ---

type contextKey string

const principalKey contextKey = "principal"

// Principal represents an authenticated identity.
type Principal struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Role        string       `json:"role"`
	Permissions []Permission `json:"permissions"`
	AuthMethod  string       `json:"auth_method"` // "api_key" or "jwt"
}

// Permission grants access to a specific resource.
type Permission struct {
	ResourceType string   `json:"resource_type"` // "stream", "schema", "pipeline", "config", "api_key"
	ResourceID   string   `json:"resource_id"`   // specific ID or "*"
	Actions      []string `json:"actions"`       // "read", "write", "admin"
}

// HasRole checks if the principal has the given role or higher.
func (p *Principal) HasRole(role string) bool {
	hierarchy := map[string]int{
		common.RoleAdmin: 100,
		"operator":       80,
		"schema_admin":   60,
		common.RoleWriter: 40,
		common.RoleReader: 20,
	}
	return hierarchy[p.Role] >= hierarchy[role]
}

// CanAccess checks if the principal can perform an action on a resource.
func (p *Principal) CanAccess(resourceType, resourceID, action string) bool {
	if p.Role == common.RoleAdmin {
		return true
	}
	for _, perm := range p.Permissions {
		if perm.ResourceType != resourceType {
			continue
		}
		if perm.ResourceID != "*" && perm.ResourceID != resourceID {
			continue
		}
		for _, a := range perm.Actions {
			if a == action || a == "admin" {
				return true
			}
		}
	}
	return false
}

// PrincipalFromContext extracts the principal from the request context.
func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(principalKey).(*Principal)
	return p, ok
}

// ContextWithPrincipal stores the principal in the context.
func ContextWithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}

// --- Auth Middleware ---

// RateLimitChecker checks if a request is allowed under rate limits.
type RateLimitChecker func(ctx context.Context, clientID string, category string) (allowed bool, retryAfter time.Duration)

// AuthzChecker checks if a principal is authorized for a resource+action.
type AuthzChecker func(ctx context.Context, principal *Principal, resourceType string, resourceID string, action string) error

// Middleware provides HTTP authentication middleware.
type Middleware struct {
	db            *sql.DB
	jwtValidator  *JWTValidator
	skipPaths     map[string]bool
	onAuthFailure func(reason, keyPrefix, ip, ua string)
	rateLimiter   RateLimitChecker
	authorizer    AuthzChecker
}

// NewMiddleware creates a new auth middleware.
func NewMiddleware(db *sql.DB, jwtValidator *JWTValidator) *Middleware {
	return &Middleware{
		db:           db,
		jwtValidator: jwtValidator,
		skipPaths: map[string]bool{
			"/health/live":  true,
			"/health/ready": true,
			"/metrics":      true,
		},
	}
}

// SetAuthFailureCallback sets a callback for auth failures (for metrics/logging).
func (m *Middleware) SetAuthFailureCallback(fn func(reason, keyPrefix, ip, ua string)) {
	m.onAuthFailure = fn
}

// SetRateLimiter sets the rate limit checker for the middleware.
func (m *Middleware) SetRateLimiter(rl RateLimitChecker) {
	m.rateLimiter = rl
}

// SetAuthorizer sets the authorization checker for the middleware.
func (m *Middleware) SetAuthorizer(az AuthzChecker) {
	m.authorizer = az
}

// Handler wraps an http.Handler with authentication.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for health/metrics endpoints
		if m.skipPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		// Rate limiting (pre-auth, by IP)
		if m.rateLimiter != nil {
			allowed, retryAfter := m.rateLimiter(r.Context(), r.RemoteAddr, "admin")
			if !allowed {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds())+1))
				writeAuthError(w, http.StatusTooManyRequests, "RATE_LIMITED", "rate limit exceeded")
				return
			}
		}

		principal, err := m.authenticate(r)
		if err != nil {
			ip := r.RemoteAddr
			ua := r.UserAgent()
			keyPrefix := ""
			if auth := r.Header.Get("Authorization"); len(auth) > 15 {
				keyPrefix = auth[:15] + "..."
			}
			if m.onAuthFailure != nil {
				m.onAuthFailure(err.Error(), keyPrefix, ip, ua)
			}
			writeAuthError(w, http.StatusUnauthorized, "AUTHENTICATION_FAILED", err.Error())
			return
		}

		// Update last_used_at for API keys (async, best-effort)
		if principal.AuthMethod == "api_key" {
			go func() {
				_, _ = m.db.Exec(
					`UPDATE api_keys SET last_used_at = NOW() WHERE key_id = $1`,
					principal.ID)
			}()
		}

		// Authorization (post-auth)
		if m.authorizer != nil {
			resType, action := restResourceAction(r)
			if err := m.authorizer(r.Context(), principal, resType, "", action); err != nil {
				writeAuthError(w, http.StatusForbidden, "AUTHORIZATION_FAILED", err.Error())
				return
			}
		}

		ctx := ContextWithPrincipal(r.Context(), principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *Middleware) authenticate(r *http.Request) (*Principal, error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}

	// API Key: "ApiKey ttd_..."
	if strings.HasPrefix(auth, "ApiKey ") {
		return m.authenticateAPIKey(r.Context(), strings.TrimPrefix(auth, "ApiKey "))
	}

	// JWT: "Bearer eyJ..."
	if strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimPrefix(auth, "Bearer ")
		if m.jwtValidator != nil {
			return m.jwtValidator.Validate(token)
		}
		return nil, fmt.Errorf("JWT authentication not configured")
	}

	return nil, fmt.Errorf("unsupported authorization scheme")
}

func (m *Middleware) authenticateAPIKey(ctx context.Context, apiKey string) (*Principal, error) {
	// Validate key format
	if !strings.HasPrefix(apiKey, "ttd_") {
		return nil, fmt.Errorf("invalid API key format")
	}

	// Hash the key
	hash := sha256.Sum256([]byte(apiKey))
	keyHash := hex.EncodeToString(hash[:])

	// Lookup in database
	var keyID, name, role string
	var streamPerms json.RawMessage
	var expiresAt *time.Time

	err := m.db.QueryRowContext(ctx,
		`SELECT key_id, name, role, stream_permissions, expires_at
		 FROM api_keys WHERE key_hash = $1`, keyHash,
	).Scan(&keyID, &name, &role, &streamPerms, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invalid API key")
	}
	if err != nil {
		return nil, fmt.Errorf("authentication error: %w", err)
	}

	// Check expiration
	if expiresAt != nil && expiresAt.Before(time.Now()) {
		return nil, fmt.Errorf("API key expired")
	}

	// Parse stream permissions
	var permissions []Permission
	if len(streamPerms) > 0 && string(streamPerms) != "[]" {
		var rawPerms []struct {
			StreamID string   `json:"stream_id"`
			Actions  []string `json:"actions"`
		}
		if err := json.Unmarshal(streamPerms, &rawPerms); err == nil {
			for _, rp := range rawPerms {
				permissions = append(permissions, Permission{
					ResourceType: "stream",
					ResourceID:   rp.StreamID,
					Actions:      rp.Actions,
				})
			}
		}
	}

	return &Principal{
		ID:          keyID,
		Name:        name,
		Role:        role,
		Permissions: permissions,
		AuthMethod:  "api_key",
	}, nil
}

// restResourceAction derives the resource type and action from the HTTP request.
func restResourceAction(r *http.Request) (resourceType string, action string) {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/api/v1/schemas"):
		resourceType = "schema"
	case strings.HasPrefix(path, "/api/v1/streams"):
		resourceType = "stream"
	case strings.HasPrefix(path, "/api/v1/pipelines"):
		resourceType = "pipeline"
	case strings.HasPrefix(path, "/api/v1/config"):
		resourceType = "config"
	case strings.HasPrefix(path, "/api/v1/api-keys"):
		resourceType = "api_key"
	default:
		resourceType = "unknown"
	}

	switch r.Method {
	case http.MethodGet:
		action = "read"
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		action = "write"
	case http.MethodDelete:
		action = "admin"
	default:
		action = "read"
	}
	return
}

func writeAuthError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
