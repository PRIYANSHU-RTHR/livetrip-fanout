package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

func newTokenServer(t *testing.T) (*httptest.Server, *auth.TokenService) {
	t.Helper()
	tokens, err := auth.NewTokenService([]byte("0123456789abcdef0123456789abcdef"), auth.TokenConfig{})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewDevTokenHandler(tokens).Register(mux)
	return httptest.NewServer(mux), tokens
}

func TestDevTokenIssuesVerifiableToken(t *testing.T) {
	srv, tokens := newTokenServer(t)
	defer srv.Close()

	got := post(t, srv, "/v1/dev/token", `{"tenant":"acme","sub":"adm","role":"office_admin","officeId":"blr-1","ttlSeconds":60}`)
	if got.status != http.StatusOK {
		t.Fatalf("status = %d %v", got.status, got.body)
	}
	tok, _ := got.body["token"].(string)
	p, err := tokens.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	want := domain.Principal{Tenant: "acme", UserID: "adm", Role: domain.RoleOfficeAdmin, OfficeID: "blr-1"}
	if p != want {
		t.Fatalf("principal = %+v, want %+v", p, want)
	}
	if _, ok := got.body["expiresAt"]; !ok {
		t.Fatalf("missing expiresAt: %v", got.body)
	}
}

func TestDevTokenRejectsBadRequests(t *testing.T) {
	srv, _ := newTokenServer(t)
	defer srv.Close()

	tests := []struct {
		name, body, code string
	}{
		{"not json", `nope`, "invalid_json"},
		{"unknown field", `{"tenant":"acme","sub":"u","role":"employee","admin":true}`, "invalid_json"},
		{"bad role", `{"tenant":"acme","sub":"u","role":"root"}`, "invalid_request"},
		{"no tenant", `{"sub":"u","role":"employee"}`, "invalid_request"},
		{"admin without office", `{"tenant":"acme","sub":"u","role":"office_admin"}`, "invalid_request"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := post(t, srv, "/v1/dev/token", tc.body)
			if got.status != http.StatusBadRequest || got.body["code"] != tc.code {
				t.Fatalf("got %d %v, want 400 %s", got.status, got.body, tc.code)
			}
		})
	}
}
