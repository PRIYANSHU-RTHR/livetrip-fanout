// Package web embeds the browser pages served by the gateway.
package web

import _ "embed"

// Demo is the three-panel live demo (security desk, employee, office admin).
//
//go:embed demo.html
var Demo []byte

// Docs renders api/openapi.yaml with the Scalar API reference.
//
//go:embed docs.html
var Docs []byte
