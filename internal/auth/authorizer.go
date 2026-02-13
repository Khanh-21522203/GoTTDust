package auth

import (
	"context"
	"fmt"
)

// Action represents an authorization action.
type Action string

const (
	ActionRead  Action = "read"
	ActionWrite Action = "write"
	ActionAdmin Action = "admin"
)

// Resource identifies a target resource for authorization.
type Resource struct {
	Type string // "stream", "schema", "pipeline", "config", "api_key"
	ID   string // specific ID or empty for collection-level
}

// Authorizer checks whether a principal can perform an action on a resource.
type Authorizer struct{}

// NewAuthorizer creates a new authorizer.
func NewAuthorizer() *Authorizer {
	return &Authorizer{}
}

// Authorize checks if the principal is allowed to perform the action on the resource.
func (a *Authorizer) Authorize(ctx context.Context, principal *Principal, resource Resource, action Action) error {
	if principal == nil {
		return fmt.Errorf("no authenticated principal")
	}

	// Admin role has full access
	if principal.Role == "admin" {
		return nil
	}

	// Role-based checks
	switch action {
	case ActionAdmin:
		if principal.Role != "admin" {
			return fmt.Errorf("admin role required for action %s on %s", action, resource.Type)
		}
	case ActionWrite:
		switch resource.Type {
		case "stream":
			if !principal.HasRole("writer") {
				return fmt.Errorf("writer role required for writing to streams")
			}
		case "schema":
			if !principal.HasRole("schema_admin") && !principal.HasRole("writer") {
				return fmt.Errorf("schema_admin or writer role required for schema writes")
			}
		case "config":
			return fmt.Errorf("admin role required for config writes")
		case "api_key":
			return fmt.Errorf("admin role required for API key management")
		}
	case ActionRead:
		switch resource.Type {
		case "stream", "schema":
			if !principal.HasRole("reader") {
				return fmt.Errorf("reader role required")
			}
		case "config":
			if !principal.HasRole("operator") {
				return fmt.Errorf("operator role required for config reads")
			}
		}
	}

	// Resource-level permission check (stream-level granularity)
	if resource.ID != "" && resource.Type == "stream" {
		if !principal.CanAccess(resource.Type, resource.ID, string(action)) {
			return fmt.Errorf("no permission for %s on %s/%s", action, resource.Type, resource.ID)
		}
	}

	return nil
}

// RequireAuth is a helper that extracts the principal and authorizes in one call.
func (a *Authorizer) RequireAuth(ctx context.Context, resource Resource, action Action) (*Principal, error) {
	principal, ok := PrincipalFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("unauthenticated")
	}
	if err := a.Authorize(ctx, principal, resource, action); err != nil {
		return nil, err
	}
	return principal, nil
}
