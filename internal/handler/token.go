package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

const maxTokenBody = 4 << 10

// TokenIssuer signs a token for a principal.
type TokenIssuer interface {
	Issue(p domain.Principal, ttl time.Duration) (string, time.Time, error)
}

// DevTokenHandler serves POST /v1/dev/token, which signs any principal it
// is asked for. It exists for demos and load tests and must only be
// registered when dev mode is switched on.
type DevTokenHandler struct {
	issuer TokenIssuer
}

func NewDevTokenHandler(issuer TokenIssuer) *DevTokenHandler {
	return &DevTokenHandler{issuer: issuer}
}

func (h *DevTokenHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/dev/token", h.token)
}

type tokenRequest struct {
	Tenant     string      `json:"tenant"`
	Sub        string      `json:"sub"`
	Role       domain.Role `json:"role"`
	OfficeID   string      `json:"officeId"`
	TTLSeconds int         `json:"ttlSeconds"`
}

type tokenResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

var errInvalidPrincipal = apiError{http.StatusBadRequest, "invalid_request",
	"need tenant, sub and a role of employee, office_admin, security or operator; office_admin needs officeId"}

func (h *DevTokenHandler) token(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxTokenBody))
	dec.DisallowUnknownFields()
	var req tokenRequest
	if err := dec.Decode(&req); err != nil || dec.Decode(&struct{}{}) != io.EOF {
		writeError(w, errInvalidJSON)
		return
	}

	p := domain.Principal{Tenant: req.Tenant, UserID: req.Sub, Role: req.Role, OfficeID: req.OfficeID}
	tok, exp, err := h.issuer.Issue(p, time.Duration(req.TTLSeconds)*time.Second)
	if err != nil {
		writeError(w, errInvalidPrincipal)
		return
	}
	writeJSON(w, http.StatusOK, tokenResponse{Token: tok, ExpiresAt: exp.UTC()})
}
