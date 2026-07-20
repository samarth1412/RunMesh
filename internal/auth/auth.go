package auth

import (
	"context"
	"net/http"
	"strings"
)

type Principal struct {
	TenantID, UserID, Role string
	Scopes                 []string
}
type contextKey struct{}

var roleLevel = map[string]int{"viewer": 1, "operator": 2, "developer": 3, "admin": 4}

func Middleware(dev bool, devPrincipal Principal) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := Principal{}
			if dev {
				p = devPrincipal
			} else {
				// These headers must be stripped and set by the deployment's trusted OIDC proxy.
				p.TenantID = r.Header.Get("X-RunMesh-Tenant-ID")
				p.UserID = r.Header.Get("X-RunMesh-User-ID")
				p.Role = r.Header.Get("X-RunMesh-Role")
				p.Scopes = strings.Fields(r.Header.Get("X-RunMesh-Scopes"))
			}
			if p.TenantID == "" || roleLevel[p.Role] == 0 {
				http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, p)))
		})
	}
}

func PrincipalFrom(ctx context.Context) Principal {
	p, _ := ctx.Value(contextKey{}).(Principal)
	return p
}

func RequireRole(minimum string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if roleLevel[PrincipalFrom(r.Context()).Role] < roleLevel[minimum] {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func Internal(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || got != token {
			http.Error(w, `{"error":"invalid internal token"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
