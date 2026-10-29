package domain

import "errors"

// Sentinel errors shared across packages. Wrap them with fmt.Errorf("%w: ...")
// for detail and match with errors.Is.
var (
	ErrBadTopic        = errors.New("bad topic")
	ErrBadEvent        = errors.New("bad event")
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrForbidden       = errors.New("forbidden")
)
