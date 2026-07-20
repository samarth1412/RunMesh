package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Principal struct {
	TenantID, UserID, Role, Kind string
	Scopes                       []string
}

type contextKey struct{}

var roleLevel = map[string]int{"viewer": 1, "operator": 2, "developer": 3, "admin": 4}

type Config struct {
	Dev            bool
	DevPrincipal   Principal
	DevWorkerToken string
	Issuer         string
	Audience       string
	JWKSURL        string
	TenantClaim    string
	APIKeyPepper   string
}

type tokenVerifier interface {
	Verify(context.Context, string) (*oidc.IDToken, error)
}

type Authenticator struct {
	pool     *pgxpool.Pool
	config   Config
	verifier tokenVerifier
}

func New(pool *pgxpool.Pool, config Config) *Authenticator {
	if config.TenantClaim == "" {
		config.TenantClaim = "runmesh_tenant_id"
	}
	a := &Authenticator{pool: pool, config: config}
	if !config.Dev {
		keys := oidc.NewRemoteKeySet(context.Background(), config.JWKSURL)
		a.verifier = oidc.NewVerifier(config.Issuer, keys, &oidc.Config{ClientID: config.Audience})
	}
	return a
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := a.authenticateRequest(r)
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "unauthenticated")
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
	})
}

func (a *Authenticator) WorkerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearer(r.Header.Get("Authorization"))
		principal, err := a.AuthenticateWorker(r.Context(), token)
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "invalid worker credential")
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
	})
}

func (a *Authenticator) authenticateRequest(r *http.Request) (Principal, error) {
	if a.config.Dev {
		principal := a.config.DevPrincipal
		principal.Kind = "development"
		return principal, nil
	}
	token := bearer(r.Header.Get("Authorization"))
	if token == "" && r.URL.Path == "/v1/stream" {
		token = webSocketBearer(r.Header.Get("Sec-WebSocket-Protocol"))
	}
	return a.Authenticate(r.Context(), token)
}

func webSocketBearer(protocols string) string {
	for _, protocol := range strings.Split(protocols, ",") {
		protocol = strings.TrimSpace(protocol)
		if strings.HasPrefix(protocol, "bearer.") {
			return strings.TrimPrefix(protocol, "bearer.")
		}
	}
	return ""
}

func (a *Authenticator) Authenticate(ctx context.Context, raw string) (Principal, error) {
	if raw == "" {
		return Principal{}, errors.New("missing bearer token")
	}
	if strings.HasPrefix(raw, "rm_") {
		return a.authenticateAPIKey(ctx, raw)
	}
	return a.authenticateOIDC(ctx, raw)
}

func (a *Authenticator) AuthenticateWorker(ctx context.Context, raw string) (Principal, error) {
	if a.config.Dev && raw != "" && subtle.ConstantTimeCompare([]byte(raw), []byte(a.config.DevWorkerToken)) == 1 {
		principal := a.config.DevPrincipal
		principal.Kind = "development"
		principal.Scopes = []string{"workers:execute"}
		return principal, nil
	}
	principal, err := a.Authenticate(ctx, raw)
	if err != nil || principal.Kind != "api_key" || !HasScope(principal, "workers:execute") {
		return Principal{}, errors.New("worker scope is required")
	}
	return principal, nil
}

func (a *Authenticator) authenticateOIDC(ctx context.Context, raw string) (Principal, error) {
	if a.verifier == nil {
		return Principal{}, errors.New("OIDC is not configured")
	}
	token, err := a.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, err
	}
	claims := map[string]any{}
	if err = token.Claims(&claims); err != nil {
		return Principal{}, err
	}
	subject, _ := claims["sub"].(string)
	tenantID, _ := claims[a.config.TenantClaim].(string)
	if subject == "" || tenantID == "" {
		return Principal{}, errors.New("required identity claims are missing")
	}
	var principal Principal
	err = a.pool.QueryRow(ctx, `SELECT id,tenant_id,role::text FROM users WHERE tenant_id=$1 AND oidc_subject=$2`, tenantID, subject).Scan(&principal.UserID, &principal.TenantID, &principal.Role)
	if err != nil {
		return Principal{}, err
	}
	principal.Kind = "oidc"
	return principal, nil
}

func (a *Authenticator) authenticateAPIKey(ctx context.Context, raw string) (Principal, error) {
	id, ok := APIKeyID(raw)
	if !ok {
		return Principal{}, errors.New("malformed API key")
	}
	var principal Principal
	var storedHash []byte
	var expiresAt *time.Time
	err := a.pool.QueryRow(ctx, `SELECT tenant_id,key_hash,scopes,expires_at FROM api_keys WHERE id=$1 AND revoked_at IS NULL`, id).Scan(&principal.TenantID, &storedHash, &principal.Scopes, &expiresAt)
	if err != nil {
		return Principal{}, err
	}
	calculated := HashAPIKey(raw, a.config.APIKeyPepper)
	if subtle.ConstantTimeCompare(calculated, storedHash) != 1 || (expiresAt != nil && !expiresAt.After(time.Now())) {
		return Principal{}, errors.New("invalid API key")
	}
	principal.Kind = "api_key"
	principal.Role = "viewer"
	_, _ = a.pool.Exec(ctx, `UPDATE api_keys SET last_used_at=now() WHERE id=$1`, id)
	return principal, nil
}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, principal)
}

func PrincipalFrom(ctx context.Context) Principal {
	principal, _ := ctx.Value(contextKey{}).(Principal)
	return principal
}

func Require(role, scope string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal := PrincipalFrom(r.Context())
		if principal.Kind == "api_key" {
			if !HasScope(principal, scope) {
				writeAuthError(w, http.StatusForbidden, "insufficient scope")
				return
			}
		} else if roleLevel[principal.Role] < roleLevel[role] {
			writeAuthError(w, http.StatusForbidden, "forbidden")
			return
		}
		next(w, r)
	}
}

func RequireHumanRole(role string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal := PrincipalFrom(r.Context())
		if principal.Kind == "api_key" || roleLevel[principal.Role] < roleLevel[role] {
			writeAuthError(w, http.StatusForbidden, "interactive administrator required")
			return
		}
		next(w, r)
	}
}

func HasScope(principal Principal, required string) bool {
	for _, scope := range principal.Scopes {
		if scope == required {
			return true
		}
	}
	return false
}

func GenerateAPIKey(pepper string) (id, token string, hash []byte, err error) {
	id = uuid.NewString()
	token, hash, err = GenerateAPIKeyForID(id, pepper)
	return id, token, hash, err
}

func GenerateAPIKeyForID(id, pepper string) (token string, hash []byte, err error) {
	if _, err = uuid.Parse(id); err != nil {
		return "", nil, err
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return "", nil, err
	}
	token = "rm_" + id + "_" + base64.RawURLEncoding.EncodeToString(secret)
	return token, HashAPIKey(token, pepper), nil
}

func APIKeyID(token string) (string, bool) {
	parts := strings.SplitN(token, "_", 3)
	if len(parts) != 3 || parts[0] != "rm" {
		return "", false
	}
	if _, err := uuid.Parse(parts[1]); err != nil || parts[2] == "" {
		return "", false
	}
	return parts[1], true
}

func HashAPIKey(token, pepper string) []byte {
	sum := sha256.Sum256([]byte(pepper + "\x00" + token))
	return sum[:]
}

func bearer(header string) string {
	if !strings.HasPrefix(header, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
}

func writeAuthError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":"` + message + `"}`))
}
