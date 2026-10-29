package domain

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestTopicRoundTrip(t *testing.T) {
	tests := []struct {
		in   string
		want Topic
	}{
		{"t:acme:trip:42", Topic{Tenant: "acme", Kind: TopicTrip, ID: "42"}},
		{"t:acme:cab:KA01-1234", Topic{Tenant: "acme", Kind: TopicCab, ID: "KA01-1234"}},
		{"t:globex:office:blr-1", Topic{Tenant: "globex", Kind: TopicOffice, ID: "blr-1"}},
		{"t:globex:sos:blr-1", Topic{Tenant: "globex", Kind: TopicSOS, ID: "blr-1"}},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseTopic(tc.in)
			if err != nil {
				t.Fatalf("ParseTopic(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("ParseTopic(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
			if s := got.String(); s != tc.in {
				t.Fatalf("String() = %q, want %q", s, tc.in)
			}
		})
	}
}

func TestParseTopicRejects(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"missing tenant", "t::trip:42"},
		{"missing id", "t:acme:trip:"},
		{"unknown kind", "t:acme:bus:42"},
		{"wrong prefix", "x:acme:trip:42"},
		{"too few parts", "t:acme:trip"},
		{"too many parts", "t:acme:trip:42:extra"},
		{"hash tag brace", "t:ac{me:trip:42"},
		{"space in id", "t:acme:trip:4 2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTopic(tc.in)
			if !errors.Is(err, ErrBadTopic) {
				t.Fatalf("ParseTopic(%q) err = %v, want ErrBadTopic", tc.in, err)
			}
		})
	}
}

func TestNewTopicRejectsMissingTenant(t *testing.T) {
	if _, err := NewTopic("", TopicTrip, "42"); !errors.Is(err, ErrBadTopic) {
		t.Fatalf("err = %v, want ErrBadTopic", err)
	}
}

func TestTopicJSON(t *testing.T) {
	in := struct {
		Topic Topic `json:"topic"`
	}{Topic{Tenant: "acme", Kind: TopicTrip, ID: "42"}}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"topic":"t:acme:trip:42"}` {
		t.Fatalf("marshal = %s", b)
	}

	var out struct {
		Topic Topic `json:"topic"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Topic != in.Topic {
		t.Fatalf("unmarshal = %+v, want %+v", out.Topic, in.Topic)
	}

	if err := json.Unmarshal([]byte(`{"topic":"t::trip:42"}`), &out); !errors.Is(err, ErrBadTopic) {
		t.Fatalf("unmarshal bad topic err = %v, want ErrBadTopic", err)
	}
}
