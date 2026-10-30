package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/handler"
)

func TestAssetsAreServed(t *testing.T) {
	mux := http.NewServeMux()
	handler.RegisterAssets(mux, assets()...)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tests := []struct {
		path, contentType, contains string
	}{
		{"/demo", "text/html; charset=utf-8", "/v1/dev/token"},
		{"/docs", "text/html; charset=utf-8", "/openapi.yaml"},
		{"/openapi.yaml", "application/yaml", "openapi: 3.1"},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			res, err := http.Get(srv.URL + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			if res.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", res.StatusCode)
			}
			if ct := res.Header.Get("Content-Type"); ct != tc.contentType {
				t.Fatalf("Content-Type = %q, want %q", ct, tc.contentType)
			}
			if !strings.Contains(string(body), tc.contains) {
				t.Fatalf("body does not mention %q", tc.contains)
			}
		})
	}

	res, err := http.Post(srv.URL+"/demo", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /demo status = %d, want 405", res.StatusCode)
	}
}
