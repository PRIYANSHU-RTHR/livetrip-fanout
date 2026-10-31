//go:build race

package handler

// raceEnabled is set under -race, which makes stacks and heap objects larger.
const raceEnabled = true
