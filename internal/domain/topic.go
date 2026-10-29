package domain

import (
	"fmt"
	"strings"
)

// TopicKind is what a topic is about.
type TopicKind string

const (
	TopicCab    TopicKind = "cab"
	TopicTrip   TopicKind = "trip"
	TopicOffice TopicKind = "office"
	TopicSOS    TopicKind = "sos"
)

func (k TopicKind) Valid() bool {
	switch k {
	case TopicCab, TopicTrip, TopicOffice, TopicSOS:
		return true
	}
	return false
}

const topicPrefix = "t"

// Topic names a stream of events inside one tenant.
// Its string form is "t:{tenant}:{kind}:{id}", e.g. "t:acme:trip:42".
type Topic struct {
	Tenant string
	Kind   TopicKind
	ID     string
}

// NewTopic builds a topic and validates it.
func NewTopic(tenant string, kind TopicKind, id string) (Topic, error) {
	t := Topic{Tenant: tenant, Kind: kind, ID: id}
	if err := t.Validate(); err != nil {
		return Topic{}, err
	}
	return t, nil
}

// ParseTopic is the inverse of Topic.String.
func ParseTopic(s string) (Topic, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 || parts[0] != topicPrefix {
		return Topic{}, fmt.Errorf("%w: want t:{tenant}:{kind}:{id}", ErrBadTopic)
	}
	return NewTopic(parts[1], TopicKind(parts[2]), parts[3])
}

func (t Topic) Validate() error {
	if !validSegment(t.Tenant) {
		return fmt.Errorf("%w: missing or invalid tenant", ErrBadTopic)
	}
	if !t.Kind.Valid() {
		return fmt.Errorf("%w: unknown kind %q", ErrBadTopic, t.Kind)
	}
	if !validSegment(t.ID) {
		return fmt.Errorf("%w: missing or invalid id", ErrBadTopic)
	}
	return nil
}

func (t Topic) String() string {
	return topicPrefix + ":" + t.Tenant + ":" + string(t.Kind) + ":" + t.ID
}

func (t Topic) IsZero() bool { return t == Topic{} }

// MarshalText lets a Topic travel as its string form in JSON.
func (t Topic) MarshalText() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return []byte(t.String()), nil
}

func (t *Topic) UnmarshalText(b []byte) error {
	p, err := ParseTopic(string(b))
	if err != nil {
		return err
	}
	*t = p
	return nil
}

// validSegment rejects empty values and anything that would break the
// colon-separated form or Redis key hash tags.
func validSegment(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	return !strings.ContainsAny(s, ":{} \t\r\n")
}
