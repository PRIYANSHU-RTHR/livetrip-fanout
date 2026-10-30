// Package scripts embeds the Redis Lua scripts so they ship inside the binary.
package scripts

import _ "embed"

// Publish dedups, sequences, stores and broadcasts one event atomically.
//
//go:embed publish.lua
var Publish string
