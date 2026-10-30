// Package api embeds the OpenAPI description of the HTTP API.
package api

import _ "embed"

// OpenAPI is openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPI []byte
