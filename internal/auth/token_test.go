package auth

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

var testSecret = []byte("0123456789abcdef0123456789abcdef")

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }
func newClock() *fakeClock               { return &fakeClock{t: time.Unix(1700000000, 0)} }
func employee(tenant, user string) domain.Principal {
	return domain.Principal{Tenant: tenant, UserID: user, Role: domain.RoleEmployee}
}

func newTokens(t *testing.T, clk *fakeClock) *TokenService {
	t.Helper()
	s, err := NewTokenService(testSecret, TokenConfig{Now: clk.now})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestTokenRoundTrip(t *testing.T) {
	s := newTokens(t, newClock())
	want := domain.Principal{Tenant: "acme", UserID: "u-1", Role: domain.RoleOfficeAdmin, OfficeID: "blr-1"}
	tok, exp, err := s.Issue(want, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := exp.Sub(time.Unix(1700000000, 0)); got != DefaultTokenTTL {
		t.Fatalf("ttl = %v, want %v", got, DefaultTokenTTL)
	}
	got, err := s.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Verify = %+v, want %+v", got, want)
	}
}

func TestTokenTTLIsCapped(t *testing.T) {
	clk := newClock()
	s := newTokens(t, clk)
	_, exp, err := s.Issue(employee("acme", "u-1"), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := exp.Sub(clk.t); got != MaxTokenTTL {
		t.Fatalf("ttl = %v, want %v", got, MaxTokenTTL)
	}
}

func TestWeakSecretRejected(t *testing.T) {
	if _, err := NewTokenService([]byte("short"), TokenConfig{}); !errors.Is(err, ErrWeakSecret) {
		t.Fatalf("err = %v, want ErrWeakSecret", err)
	}
}

func TestIssueRejectsBadPrincipal(t *testing.T) {
	s := newTokens(t, newClock())
	bad := []domain.Principal{
		{Tenant: "", UserID: "u", Role: domain.RoleEmployee},
		{Tenant: "ac:me", UserID: "u", Role: domain.RoleEmployee},
		{Tenant: "acme", UserID: "", Role: domain.RoleEmployee},
		{Tenant: "acme", UserID: "u", Role: "admin"},
		{Tenant: "acme", UserID: "u", Role: domain.RoleOfficeAdmin},
	}
	for _, p := range bad {
		if _, _, err := s.Issue(p, 0); err == nil {
			t.Errorf("Issue(%+v) succeeded", p)
		}
	}
}

func TestVerifyRejects(t *testing.T) {
	clk := newClock()
	s := newTokens(t, clk)
	good, _, err := s.Issue(employee("acme", "u-1"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	other, _ := NewTokenService([]byte("ffffffffffffffffffffffffffffffff"), TokenConfig{Now: clk.now})
	wrongKey, _, _ := other.Issue(employee("acme", "u-1"), time.Minute)

	otherIss, _ := NewTokenService(testSecret, TokenConfig{Issuer: "someone-else", Now: clk.now})
	wrongIssuer, _, _ := otherIss.Issue(employee("acme", "u-1"), time.Minute)

	// "alg: none" with the same claims must never be accepted.
	none, _ := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"iss": defaultIssuer, "sub": "u-1", "tenant": "acme", "role": "operator",
		"exp": clk.t.Add(time.Hour).Unix(),
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)

	noExp, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": defaultIssuer, "sub": "u-1", "tenant": "acme", "role": "operator",
	}).SignedString(testSecret)

	badRole, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": defaultIssuer, "sub": "u-1", "tenant": "acme", "role": "root",
		"exp": clk.t.Add(time.Hour).Unix(),
	}).SignedString(testSecret)

	// Change the tenant in the payload but keep the old signature.
	parts := strings.Split(good, ".")
	forged, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": defaultIssuer, "sub": "u-1", "tenant": "globex", "role": "employee",
		"exp": clk.t.Add(time.Hour).Unix(),
	}).SignedString([]byte("not-the-secret-not-the-secret-xx"))
	tampered := parts[0] + "." + strings.Split(forged, ".")[1] + "." + parts[2]

	tests := map[string]string{
		"empty":        "",
		"garbage":      "not.a.token",
		"wrong key":    wrongKey,
		"wrong issuer": wrongIssuer,
		"alg none":     none,
		"no exp":       noExp,
		"bad role":     badRole,
		"tampered":     tampered,
	}
	for name, tok := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Verify(tok); !errors.Is(err, domain.ErrUnauthenticated) {
				t.Fatalf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}

	t.Run("expired", func(t *testing.T) {
		clk.add(time.Minute + clockLeeway + time.Second)
		if _, err := s.Verify(good); !errors.Is(err, domain.ErrUnauthenticated) {
			t.Fatalf("err = %v, want ErrUnauthenticated", err)
		}
	})
}
