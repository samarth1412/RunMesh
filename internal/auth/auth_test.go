package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRoleEnforcement(t *testing.T) {
	handler := Middleware(true, Principal{TenantID: "tenant", UserID: "user", Role: "viewer"})(RequireRole("operator", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d", response.Code)
	}
}
func TestPrincipalComesFromIdentityMiddleware(t *testing.T) {
	handler := Middleware(true, Principal{TenantID: "trusted", UserID: "user", Role: "admin"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if PrincipalFrom(r.Context()).TenantID != "trusted" {
			t.Error("tenant was not derived from identity")
		}
		w.WriteHeader(204)
	}))
	request := httptest.NewRequest(http.MethodGet, "/?tenant_id=attacker", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 204 {
		t.Fatalf("status=%d", response.Code)
	}
}
