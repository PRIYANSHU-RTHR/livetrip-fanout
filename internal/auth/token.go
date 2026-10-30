package auth

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

const (
	// MinSecretLen is the shortest HS256 key accepted (256 bits).
	MinSecretLen = 32
	// DefaultTokenTTL is how long an issued token stays valid.
	DefaultTokenTTL = time.Hour
	// MaxTokenTTL caps the lifetime a caller may ask for.
	MaxTokenTTL   = 24 * time.Hour
	defaultIssuer = "livetrip"
	clockLeeway   = 5 * time.Second
)

// ErrWeakSecret means the signing key is too short to be safe.
var ErrWeakSecret = errors.New("jwt secret must be at least 32 bytes")

// claims is the token body. Tenant and role are signed, so a client cannot
// widen its own access by editing them.
type claims struct {
	Tenant   string      `json:"tenant"`
	Role     domain.Role `json:"role"`
	OfficeID string      `json:"officeId,omitempty"`
	jwt.RegisteredClaims
}

// TokenService issues and verifies HS256 tokens. HS256 keeps the demo
// self-contained; production would verify RS256/ES256 tokens from an IdP.
type TokenService struct {
	secret []byte
	issuer string
	now    func() time.Time
}

// TokenConfig tunes a TokenService. Zero values take the defaults.
type TokenConfig struct {
	Issuer string
	// Now is the clock, replaceable in tests.
	Now func() time.Time
}

func NewTokenService(secret []byte, cfg TokenConfig) (*TokenService, error) {
	if len(secret) < MinSecretLen {
		return nil, ErrWeakSecret
	}
	if cfg.Issuer == "" {
		cfg.Issuer = defaultIssuer
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &TokenService{secret: append([]byte(nil), secret...), issuer: cfg.Issuer, now: cfg.Now}, nil
}

// Issue signs a token for p. ttl <= 0 takes DefaultTokenTTL.
func (s *TokenService) Issue(p domain.Principal, ttl time.Duration) (string, time.Time, error) {
	if err := ValidatePrincipal(p); err != nil {
		return "", time.Time{}, err
	}
	if ttl <= 0 {
		ttl = DefaultTokenTTL
	}
	if ttl > MaxTokenTTL {
		ttl = MaxTokenTTL
	}
	now := s.now()
	exp := now.Add(ttl)
	c := claims{
		Tenant:   p.Tenant,
		Role:     p.Role,
		OfficeID: p.OfficeID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   p.UserID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(s.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign token: %w", err)
	}
	return tok, exp, nil
}

// Verify checks signature, algorithm, issuer and expiry, and returns the
// principal. Every failure is ErrUnauthenticated; the cause is wrapped for
// logs only.
func (s *TokenService) Verify(token string) (domain.Principal, error) {
	var c claims
	_, err := jwt.ParseWithClaims(token, &c,
		func(*jwt.Token) (any, error) { return s.secret, nil },
		// Pinning the algorithm stops "alg: none" and key-confusion tricks.
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(s.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(clockLeeway),
		jwt.WithTimeFunc(s.now),
	)
	if err != nil {
		return domain.Principal{}, fmt.Errorf("%w: %w", domain.ErrUnauthenticated, err)
	}
	p := domain.Principal{Tenant: c.Tenant, UserID: c.Subject, Role: c.Role, OfficeID: c.OfficeID}
	if err := ValidatePrincipal(p); err != nil {
		return domain.Principal{}, fmt.Errorf("%w: %w", domain.ErrUnauthenticated, err)
	}
	return p, nil
}

// ValidatePrincipal checks the fields every role needs. An office admin
// must name the office it administers.
func ValidatePrincipal(p domain.Principal) error {
	if _, err := domain.NewTopic(p.Tenant, domain.TopicTrip, "x"); err != nil {
		return errors.New("invalid tenant")
	}
	if p.UserID == "" || strings.TrimSpace(p.UserID) != p.UserID {
		return errors.New("invalid subject")
	}
	if !p.Role.Valid() {
		return errors.New("invalid role")
	}
	if p.Role == domain.RoleOfficeAdmin && p.OfficeID == "" {
		return errors.New("office_admin needs officeId")
	}
	return nil
}
