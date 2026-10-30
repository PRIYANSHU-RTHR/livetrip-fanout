package handler

import (
	"net/http"
	"strconv"
)

// Asset is a fixed file compiled into the binary and served as-is.
type Asset struct {
	Path        string
	ContentType string
	Body        []byte
}

// RegisterAssets serves each asset at GET Path.
func RegisterAssets(mux *http.ServeMux, assets ...Asset) {
	for _, a := range assets {
		mux.HandleFunc("GET "+a.Path, func(w http.ResponseWriter, _ *http.Request) {
			h := w.Header()
			h.Set("Content-Type", a.ContentType)
			h.Set("Content-Length", strconv.Itoa(len(a.Body)))
			h.Set("X-Content-Type-Options", "nosniff")
			// The files change only with a new build; revalidate each time.
			h.Set("Cache-Control", "no-cache")
			_, _ = w.Write(a.Body)
		})
	}
}
