package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRoleAndScopeEnforcement(t *testing.T) {
	tests := []struct {
		name      string
		principal Principal
		status    int
	}{
		{name: "OIDC role accepted", principal: Principal{Kind: "oidc", Role: "operator"}, status: http.StatusNoContent},
		{name: "OIDC role rejected", principal: Principal{Kind: "oidc", Role: "viewer"}, status: http.StatusForbidden},
		{name: "API key scope accepted", principal: Principal{Kind: "api_key", Scopes: []string{"runs:execute"}}, status: http.StatusNoContent},
		{name: "API key scope rejected", principal: Principal{Kind: "api_key", Scopes: []string{"runs:read"}}, status: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := Require("operator", "runs:execute", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			request := httptest.NewRequest(http.MethodPost, "/v1/runs/id/cancel", nil)
			request = request.WithContext(WithPrincipal(request.Context(), tt.principal))
			response := httptest.NewRecorder()
			handler(response, request)
			if response.Code != tt.status {
				t.Fatalf("status=%d want=%d", response.Code, tt.status)
			}
		})
	}
}

func TestDevelopmentPrincipal(t *testing.T) {
	authenticator := New(nil, Config{Dev: true, DevPrincipal: Principal{TenantID: "tenant", UserID: "user", Role: "admin"}})
	handler := authenticator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal := PrincipalFrom(r.Context())
		if principal.TenantID != "tenant" || principal.UserID != "user" || principal.Kind != "development" {
			t.Fatalf("unexpected principal: %+v", principal)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestAPIKeyFormatAndHash(t *testing.T) {
	id, token, hash, err := GenerateAPIKey("pepper")
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok := APIKeyID(token)
	if !ok || parsed != id {
		t.Fatalf("parsed id=%q ok=%v", parsed, ok)
	}
	if string(hash) != string(HashAPIKey(token, "pepper")) || string(hash) == string(HashAPIKey(token, "other")) {
		t.Fatal("API key hashing did not bind the token to its pepper")
	}
	rotated, _, err := GenerateAPIKeyForID(id, "pepper")
	if err != nil {
		t.Fatal(err)
	}
	if rotatedID, ok := APIKeyID(rotated); !ok || rotatedID != id || rotated == token {
		t.Fatalf("rotated token did not preserve id: %q", rotated)
	}
}
