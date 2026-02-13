package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// GRPCInterceptor provides gRPC authentication via unary and stream interceptors.
type GRPCInterceptor struct {
	db            *sql.DB
	jwtValidator  *JWTValidator
	skipMethods   map[string]bool
	onAuthFailure func(reason, keyPrefix, ip, ua string)
	rateLimiter   RateLimitChecker
	authorizer    AuthzChecker
}

// NewGRPCInterceptor creates a new gRPC auth interceptor.
func NewGRPCInterceptor(db *sql.DB, jwtValidator *JWTValidator) *GRPCInterceptor {
	return &GRPCInterceptor{
		db:           db,
		jwtValidator: jwtValidator,
		skipMethods: map[string]bool{
			"/grpc.health.v1.Health/Check": true,
			"/grpc.health.v1.Health/Watch": true,
		},
	}
}

// SetAuthFailureCallback sets a callback for auth failures.
func (i *GRPCInterceptor) SetAuthFailureCallback(fn func(reason, keyPrefix, ip, ua string)) {
	i.onAuthFailure = fn
}

// SetRateLimiter sets the rate limit checker for the interceptor.
func (i *GRPCInterceptor) SetRateLimiter(rl RateLimitChecker) {
	i.rateLimiter = rl
}

// SetAuthorizer sets the authorization checker for the interceptor.
func (i *GRPCInterceptor) SetAuthorizer(az AuthzChecker) {
	i.authorizer = az
}

// UnaryInterceptor returns a gRPC unary server interceptor for authentication.
func (i *GRPCInterceptor) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if i.skipMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		// Rate limiting (pre-auth, by peer address)
		if i.rateLimiter != nil {
			category := grpcMethodCategory(info.FullMethod)
			clientID := grpcPeerAddr(ctx)
			allowed, _ := i.rateLimiter(ctx, clientID, category)
			if !allowed {
				return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded")
			}
		}

		principal, err := i.authenticateFromMetadata(ctx)
		if err != nil {
			if i.onAuthFailure != nil {
				i.onAuthFailure(err.Error(), "", "", "")
			}
			return nil, status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}

		// Authorization (post-auth)
		if i.authorizer != nil {
			resType, action := grpcResourceAction(info.FullMethod)
			if err := i.authorizer(ctx, principal, resType, "", action); err != nil {
				return nil, status.Errorf(codes.PermissionDenied, "authorization failed: %v", err)
			}
		}

		ctx = ContextWithPrincipal(ctx, principal)
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream server interceptor for authentication.
func (i *GRPCInterceptor) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if i.skipMethods[info.FullMethod] {
			return handler(srv, ss)
		}

		// Rate limiting (pre-auth, by peer address)
		if i.rateLimiter != nil {
			category := grpcMethodCategory(info.FullMethod)
			clientID := grpcPeerAddr(ss.Context())
			allowed, _ := i.rateLimiter(ss.Context(), clientID, category)
			if !allowed {
				return status.Errorf(codes.ResourceExhausted, "rate limit exceeded")
			}
		}

		principal, err := i.authenticateFromMetadata(ss.Context())
		if err != nil {
			if i.onAuthFailure != nil {
				i.onAuthFailure(err.Error(), "", "", "")
			}
			return status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}

		// Authorization (post-auth)
		if i.authorizer != nil {
			resType, action := grpcResourceAction(info.FullMethod)
			if err := i.authorizer(ss.Context(), principal, resType, "", action); err != nil {
				return status.Errorf(codes.PermissionDenied, "authorization failed: %v", err)
			}
		}

		wrapped := &authenticatedStream{
			ServerStream: ss,
			ctx:          ContextWithPrincipal(ss.Context(), principal),
		}
		return handler(srv, wrapped)
	}
}

func (i *GRPCInterceptor) authenticateFromMetadata(ctx context.Context) (*Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	// Check "authorization" metadata key
	authValues := md.Get("authorization")
	if len(authValues) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "missing authorization metadata")
	}

	auth := authValues[0]

	// API Key: "ApiKey ttd_..."
	if strings.HasPrefix(auth, "ApiKey ") {
		return i.authenticateAPIKey(ctx, strings.TrimPrefix(auth, "ApiKey "))
	}

	// JWT: "Bearer eyJ..."
	if strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimPrefix(auth, "Bearer ")
		if i.jwtValidator != nil {
			return i.jwtValidator.Validate(token)
		}
		return nil, status.Errorf(codes.Unauthenticated, "JWT authentication not configured")
	}

	return nil, status.Errorf(codes.Unauthenticated, "unsupported authorization scheme")
}

func (i *GRPCInterceptor) authenticateAPIKey(ctx context.Context, apiKey string) (*Principal, error) {
	if !strings.HasPrefix(apiKey, "ttd_") {
		return nil, status.Errorf(codes.Unauthenticated, "invalid API key format")
	}

	hash := sha256.Sum256([]byte(apiKey))
	keyHash := hex.EncodeToString(hash[:])

	var keyID, name, role string
	var streamPerms json.RawMessage
	var expiresAt *time.Time

	err := i.db.QueryRowContext(ctx,
		`SELECT key_id, name, role, stream_permissions, expires_at
		 FROM api_keys WHERE key_hash = $1`, keyHash,
	).Scan(&keyID, &name, &role, &streamPerms, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.Unauthenticated, "invalid API key")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "authentication error")
	}

	if expiresAt != nil && expiresAt.Before(time.Now()) {
		return nil, status.Errorf(codes.Unauthenticated, "API key expired")
	}

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

// grpcMethodCategory maps a gRPC method to a rate limit category.
func grpcMethodCategory(fullMethod string) string {
	switch {
	case strings.Contains(fullMethod, "Ingestion"):
		return "ingestion"
	case strings.Contains(fullMethod, "Query"):
		return "query"
	default:
		return "admin"
	}
}

// grpcPeerAddr extracts the peer address from the gRPC context.
func grpcPeerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

// grpcResourceAction maps a gRPC method to a resource type and action.
func grpcResourceAction(fullMethod string) (resourceType string, action string) {
	switch {
	case strings.Contains(fullMethod, "Ingest"):
		return "stream", "write"
	case strings.Contains(fullMethod, "Query") || strings.Contains(fullMethod, "Read"):
		return "stream", "read"
	case strings.Contains(fullMethod, "BatchJob"):
		return "stream", "read"
	default:
		return "stream", "read"
	}
}

// authenticatedStream wraps a grpc.ServerStream with an authenticated context.
type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context {
	return s.ctx
}
