package auth

import (
	"context"
	"testing"
)

func TestPrincipalHasRole(t *testing.T) {
	tests := []struct {
		name       string
		role       string
		checkRole  string
		expected   bool
	}{
		{"admin has admin", "admin", "admin", true},
		{"admin has writer", "admin", "writer", true},
		{"admin has reader", "admin", "reader", true},
		{"writer has writer", "writer", "writer", true},
		{"writer has reader", "writer", "reader", true},
		{"writer not admin", "writer", "admin", false},
		{"reader has reader", "reader", "reader", true},
		{"reader not writer", "reader", "writer", false},
		{"operator has writer", "operator", "writer", true},
		{"schema_admin has writer", "schema_admin", "writer", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Principal{Role: tc.role}
			if got := p.HasRole(tc.checkRole); got != tc.expected {
				t.Errorf("Principal{Role:%q}.HasRole(%q) = %v, want %v",
					tc.role, tc.checkRole, got, tc.expected)
			}
		})
	}
}

func TestPrincipalCanAccess(t *testing.T) {
	p := &Principal{
		Role: "writer",
		Permissions: []Permission{
			{ResourceType: "stream", ResourceID: "str-123", Actions: []string{"read", "write"}},
			{ResourceType: "stream", ResourceID: "*", Actions: []string{"read"}},
		},
	}

	tests := []struct {
		name         string
		resourceType string
		resourceID   string
		action       string
		expected     bool
	}{
		{"specific stream write", "stream", "str-123", "write", true},
		{"specific stream read", "stream", "str-123", "read", true},
		{"wildcard stream read", "stream", "str-999", "read", true},
		{"wildcard stream no write", "stream", "str-999", "write", false},
		{"wrong resource type", "schema", "sch-1", "read", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.CanAccess(tc.resourceType, tc.resourceID, tc.action); got != tc.expected {
				t.Errorf("CanAccess(%q, %q, %q) = %v, want %v",
					tc.resourceType, tc.resourceID, tc.action, got, tc.expected)
			}
		})
	}
}

func TestPrincipalCanAccessAdmin(t *testing.T) {
	admin := &Principal{Role: "admin"}
	if !admin.CanAccess("stream", "any", "write") {
		t.Error("admin should have access to everything")
	}
	if !admin.CanAccess("config", "", "admin") {
		t.Error("admin should have access to config")
	}
}

func TestAuthorizerAuthorize(t *testing.T) {
	authz := NewAuthorizer()

	tests := []struct {
		name      string
		principal *Principal
		resource  Resource
		action    Action
		wantErr   bool
	}{
		{
			"admin can do anything",
			&Principal{Role: "admin"},
			Resource{Type: "config", ID: ""},
			ActionAdmin,
			false,
		},
		{
			"writer can write stream",
			&Principal{Role: "writer", Permissions: []Permission{
				{ResourceType: "stream", ResourceID: "*", Actions: []string{"write"}},
			}},
			Resource{Type: "stream", ID: "str-1"},
			ActionWrite,
			false,
		},
		{
			"reader cannot write stream",
			&Principal{Role: "reader"},
			Resource{Type: "stream", ID: "str-1"},
			ActionWrite,
			true,
		},
		{
			"reader can read stream",
			&Principal{Role: "reader"},
			Resource{Type: "stream", ID: ""},
			ActionRead,
			false,
		},
		{
			"writer cannot admin config",
			&Principal{Role: "writer"},
			Resource{Type: "config", ID: ""},
			ActionAdmin,
			true,
		},
		{
			"nil principal",
			nil,
			Resource{Type: "stream"},
			ActionRead,
			true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := authz.Authorize(context.Background(), tc.principal, tc.resource, tc.action)
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
		})
	}
}

func TestRequireAuth(t *testing.T) {
	authz := NewAuthorizer()

	// No principal in context
	_, err := authz.RequireAuth(context.Background(), Resource{Type: "stream"}, ActionRead)
	if err == nil {
		t.Error("expected error for missing principal")
	}

	// With principal in context
	ctx := ContextWithPrincipal(context.Background(), &Principal{
		ID:   "user-1",
		Role: "admin",
	})
	p, err := authz.RequireAuth(ctx, Resource{Type: "stream"}, ActionRead)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if p.ID != "user-1" {
		t.Errorf("expected principal ID 'user-1', got %q", p.ID)
	}
}

func TestPrincipalContext(t *testing.T) {
	ctx := context.Background()

	// No principal
	_, ok := PrincipalFromContext(ctx)
	if ok {
		t.Error("expected no principal in empty context")
	}

	// With principal
	p := &Principal{ID: "test-user", Role: "writer"}
	ctx = ContextWithPrincipal(ctx, p)
	got, ok := PrincipalFromContext(ctx)
	if !ok {
		t.Error("expected principal in context")
	}
	if got.ID != "test-user" {
		t.Errorf("expected ID 'test-user', got %q", got.ID)
	}
}
