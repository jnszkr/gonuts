// Package spec parses and validates nuts.yaml, the single description of what
// a system does on NATS.
package spec

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Spec is a parsed nuts.yaml.
type Spec struct {
	Version   int                     `yaml:"version"`
	Name      string                  `yaml:"name"`
	Go        GoConfig                `yaml:"go"`
	Encoding  string                  `yaml:"encoding"`
	Models    map[string]*Model       `yaml:"models"`
	Events    map[string]Event        `yaml:"events"`
	Streams   map[string]Stream       `yaml:"streams"`
	Consumers map[string]Consumer     `yaml:"consumers"`
	KV        map[string]KVBucket     `yaml:"kv"`
	Schedules map[string]Schedule     `yaml:"schedules"`
	Requests  map[string]RequestGroup `yaml:"requests"`
	Services  map[string]Service      `yaml:"services"`
}

// GoConfig holds Go generation settings.
type GoConfig struct {
	Package string `yaml:"package"`
}

// Model is a message type. Fields keep their YAML order.
type Model struct {
	Description string
	Fields      []*Field
}

// Field is one field of a model.
type Field struct {
	Name        string
	Type        string `yaml:"type"`
	Optional    bool   `yaml:"optional"`
	Description string `yaml:"description"`
}

// Event binds a subject to a model.
type Event struct {
	Subject     string `yaml:"subject"`
	Model       string `yaml:"model"`
	Description string `yaml:"description"`
}

// Stream holds all JetStream stream settings.
type Stream struct {
	Events                 []string          `yaml:"events"`
	Description            string            `yaml:"description"`
	Subjects               []string          `yaml:"subjects"`
	Retention              string            `yaml:"retention"`
	MaxConsumers           int               `yaml:"max_consumers"`
	MaxMsgs                int64             `yaml:"max_msgs"`
	MaxBytes               int64             `yaml:"max_bytes"`
	Discard                string            `yaml:"discard"`
	DiscardNewPerSubject   bool              `yaml:"discard_new_per_subject"`
	MaxAge                 Duration          `yaml:"max_age"`
	MaxMsgsPerSubject      int64             `yaml:"max_msgs_per_subject"`
	MaxMsgSize             int32             `yaml:"max_msg_size"`
	Storage                string            `yaml:"storage"`
	Replicas               int               `yaml:"replicas"`
	NoAck                  bool              `yaml:"no_ack"`
	Duplicates             Duration          `yaml:"duplicate_window"`
	Placement              *Placement        `yaml:"placement"`
	Mirror                 *StreamSource     `yaml:"mirror"`
	Sources                []StreamSource    `yaml:"sources"`
	Sealed                 bool              `yaml:"sealed"`
	DenyDelete             bool              `yaml:"deny_delete"`
	DenyPurge              bool              `yaml:"deny_purge"`
	AllowRollup            bool              `yaml:"allow_rollup_hdrs"`
	Compression            string            `yaml:"compression"`
	FirstSeq               uint64            `yaml:"first_seq"`
	SubjectTransform       *SubjectTransform `yaml:"subject_transform"`
	RePublish              *RePublish        `yaml:"republish"`
	AllowDirect            bool              `yaml:"allow_direct"`
	MirrorDirect           bool              `yaml:"mirror_direct"`
	ConsumerLimits         *ConsumerLimits   `yaml:"consumer_limits"`
	Metadata               map[string]string `yaml:"metadata"`
	AllowMsgTTL            bool              `yaml:"allow_msg_ttl"`
	SubjectDeleteMarkerTTL Duration          `yaml:"subject_delete_marker_ttl"`
	AllowMsgCounter        bool              `yaml:"allow_msg_counter"`
	AllowAtomic            bool              `yaml:"allow_atomic"`
	AllowMsgSchedules      bool              `yaml:"allow_msg_schedules"`
	PersistMode            string            `yaml:"persist_mode"`
	AllowBatched           bool              `yaml:"allow_batched"`
}

// Placement says which servers may hold a stream.
type Placement struct {
	Cluster   string   `yaml:"cluster"`
	Tags      []string `yaml:"tags"`
	Preferred string   `yaml:"preferred"`
}

// StreamSource is a mirror or a source of another stream.
type StreamSource struct {
	Name          string     `yaml:"name"`
	FilterSubject string     `yaml:"filter_subject"`
	StartSeq      uint64     `yaml:"start_seq"`
	StartTime     *time.Time `yaml:"start_time"`
}

// SubjectTransform rewrites subjects as messages enter a stream.
type SubjectTransform struct {
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
}

// RePublish republishes stored messages to another subject.
type RePublish struct {
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
	HeadersOnly bool   `yaml:"headers_only"`
}

// ConsumerLimits are stream-wide limits on consumers.
type ConsumerLimits struct {
	InactiveThreshold Duration `yaml:"inactive_threshold"`
	MaxAckPending     int      `yaml:"max_ack_pending"`
}

// Consumer holds all JetStream consumer settings. The YAML key is its name.
type Consumer struct {
	Stream            string            `yaml:"stream"`
	Events            []string          `yaml:"events"`
	Durable           *bool             `yaml:"durable"`
	Description       string            `yaml:"description"`
	DeliverPolicy     string            `yaml:"deliver_policy"`
	OptStartSeq       uint64            `yaml:"opt_start_seq"`
	OptStartTime      *time.Time        `yaml:"opt_start_time"`
	AckPolicy         string            `yaml:"ack_policy"`
	AckWait           Duration          `yaml:"ack_wait"`
	MaxDeliver        int               `yaml:"max_deliver"`
	BackOff           []Duration        `yaml:"backoff"`
	FilterSubject     string            `yaml:"filter_subject"`
	ReplayPolicy      string            `yaml:"replay_policy"`
	RateLimit         uint64            `yaml:"rate_limit_bps"`
	SampleFreq        string            `yaml:"sample_freq"`
	MaxWaiting        int               `yaml:"max_waiting"`
	MaxAckPending     int               `yaml:"max_ack_pending"`
	HeadersOnly       bool              `yaml:"headers_only"`
	MaxBatch          int               `yaml:"max_batch"`
	MaxExpires        Duration          `yaml:"max_expires"`
	MaxBytes          int               `yaml:"max_bytes"`
	InactiveThreshold Duration          `yaml:"inactive_threshold"`
	Replicas          int               `yaml:"replicas"`
	MemoryStorage     bool              `yaml:"memory_storage"`
	FilterSubjects    []string          `yaml:"filter_subjects"`
	Metadata          map[string]string `yaml:"metadata"`
	PauseUntil        *time.Time        `yaml:"pause_until"`
	PriorityPolicy    string            `yaml:"priority_policy"`
	PriorityTimeout   Duration          `yaml:"priority_timeout"`
	PriorityGroups    []string          `yaml:"priority_groups"`
	DeliverSubject    string            `yaml:"deliver_subject"`
	DeliverGroup      string            `yaml:"deliver_group"`
	FlowControl       bool              `yaml:"flow_control"`
	IdleHeartbeat     Duration          `yaml:"idle_heartbeat"`
}

// KVBucket is a key-value bucket. The YAML key is its name.
type KVBucket struct {
	Key            string            `yaml:"key"`
	Model          string            `yaml:"model"`
	Description    string            `yaml:"description"`
	MaxValueSize   int32             `yaml:"max_value_size"`
	History        uint8             `yaml:"history"`
	TTL            Duration          `yaml:"ttl"`
	MaxBytes       int64             `yaml:"max_bytes"`
	Storage        string            `yaml:"storage"`
	Replicas       int               `yaml:"replicas"`
	Placement      *Placement        `yaml:"placement"`
	RePublish      *RePublish        `yaml:"republish"`
	Mirror         *StreamSource     `yaml:"mirror"`
	Sources        []StreamSource    `yaml:"sources"`
	Compression    bool              `yaml:"compression"`
	LimitMarkerTTL Duration          `yaml:"limit_marker_ttl"`
	Metadata       map[string]string `yaml:"metadata"`
}

// Schedule publishes an event later or repeatedly from a schedule subject.
type Schedule struct {
	Stream   string         `yaml:"stream"`
	Subject  string         `yaml:"subject"`
	Event    string         `yaml:"event"`
	Payload  map[string]any `yaml:"payload"`
	Cron     string         `yaml:"cron"`
	Every    Duration       `yaml:"every"`
	TimeZone string         `yaml:"time_zone"`
}

// RequestGroup is a set of request-reply endpoints served together.
type RequestGroup struct {
	Version    string              `yaml:"version"`
	Timeout    Duration            `yaml:"timeout"`
	QueueGroup string              `yaml:"queue_group"`
	Endpoints  map[string]Endpoint `yaml:"endpoints"`
}

// Endpoint is one request, its reply and its errors.
type Endpoint struct {
	Subject string   `yaml:"subject"`
	Request string   `yaml:"request"`
	Reply   string   `yaml:"reply"`
	Errors  []string `yaml:"errors"`
}

// Service lists what one application does on NATS.
type Service struct {
	Publishes  []string          `yaml:"publishes"`
	Subscribes []string          `yaml:"subscribes"`
	Consumes   []string          `yaml:"consumes"`
	Serves     []string          `yaml:"serves"`
	Calls      []string          `yaml:"calls"`
	KV         map[string]string `yaml:"kv"`
	Schedules  []string          `yaml:"schedules"`
}

// Duration is a time.Duration written as a string such as "30s" or "720h".
type Duration struct {
	time.Duration
}

// UnmarshalYAML parses a duration string.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	parsed, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q", n.Line, n.Value)
	}
	d.Duration = parsed
	return nil
}

// UnmarshalYAML reads a model, keeping its fields in file order.
func (m *Model) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: a model must be a mapping", n.Line)
	}
	for i := 0; i < len(n.Content); i += 2 {
		key, val := n.Content[i], n.Content[i+1]
		switch key.Value {
		case "description":
			m.Description = val.Value
		case "fields":
			fields, err := parseFields(val)
			if err != nil {
				return err
			}
			m.Fields = fields
		default:
			return fmt.Errorf("line %d: unknown model key %q", key.Line, key.Value)
		}
	}
	return nil
}

func parseFields(n *yaml.Node) ([]*Field, error) {
	if n.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("line %d: fields must be a mapping", n.Line)
	}
	fields := make([]*Field, 0, len(n.Content)/2)
	for i := 0; i < len(n.Content); i += 2 {
		key, val := n.Content[i], n.Content[i+1]
		f := &Field{Name: key.Value}
		switch val.Kind {
		case yaml.ScalarNode:
			f.Type = val.Value
		case yaml.MappingNode:
			if err := val.Decode(f); err != nil {
				return nil, fmt.Errorf("field %q: %w", key.Value, err)
			}
		default:
			return nil, fmt.Errorf("line %d: field %q must be a type or a mapping", val.Line, key.Value)
		}
		fields = append(fields, f)
	}
	return fields, nil
}

// Parse reads a nuts.yaml. It rejects unknown keys so typos cannot pass silently.
func Parse(data []byte) (*Spec, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	s := &Spec{}
	if err := dec.Decode(s); err != nil {
		return nil, fmt.Errorf("parse spec: %w", err)
	}
	return s, nil
}

// Error is one validation problem at a path in the spec.
type Error struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

func (e Error) Error() string { return e.Path + ": " + e.Message }

// Join combines validation errors into one error, or nil if there are none.
func Join(errs []Error) error {
	joined := make([]error, len(errs))
	for i, e := range errs {
		joined[i] = e
	}
	return errors.Join(joined...)
}

// SortedKeys returns a map's keys in order, so all output is deterministic.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// StreamOf returns the stream that stores an event, or "" for a core event.
func (s *Spec) StreamOf(event string) string {
	for _, name := range SortedKeys(s.Streams) {
		if slices.Contains(s.Streams[name].Events, event) {
			return name
		}
	}
	return ""
}

// EventFilter returns the wildcard form of an event's subject.
func (s *Spec) EventFilter(event string) string {
	sub, err := ParseSubject(s.Events[event].Subject)
	if err != nil {
		return ""
	}
	return sub.Wildcard()
}

// StreamSubjects returns a stream's subjects: its events' first, then explicit ones.
func (s *Spec) StreamSubjects(name string) []string {
	st := s.Streams[name]
	subjects := make([]string, 0, len(st.Events)+len(st.Subjects))
	for _, e := range st.Events {
		if f := s.EventFilter(e); f != "" {
			subjects = append(subjects, f)
		}
	}
	return append(subjects, st.Subjects...)
}

// Subject is a parsed subject template such as orders.{region}.placed.
type Subject struct {
	tokens []string
}

// ParseSubject parses a subject that may hold {param} tokens or NATS wildcards.
func ParseSubject(raw string) (Subject, error) {
	if raw == "" {
		return Subject{}, errors.New("invalid subject: empty")
	}
	tokens := strings.Split(raw, ".")
	seen := map[string]bool{}
	for _, tok := range tokens {
		if !validToken(tok) {
			return Subject{}, fmt.Errorf("invalid subject %q: bad token %q", raw, tok)
		}
		if p, ok := paramOf(tok); ok {
			if seen[p] {
				return Subject{}, fmt.Errorf("invalid subject %q: repeated param %q", raw, p)
			}
			seen[p] = true
		}
	}
	return Subject{tokens: tokens}, nil
}

func validToken(tok string) bool {
	if tok == "" {
		return false
	}
	if p, ok := paramOf(tok); ok {
		return isSnakeIdent(p)
	}
	return !strings.ContainsAny(tok, " \t\r\n{}")
}

func paramOf(tok string) (string, bool) {
	if strings.HasPrefix(tok, "{") && strings.HasSuffix(tok, "}") {
		return tok[1 : len(tok)-1], true
	}
	return "", false
}

func isSnakeIdent(s string) bool {
	if s == "" || s[0] >= '0' && s[0] <= '9' {
		return false
	}
	for _, r := range s {
		if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// Params lists the subject's parameter names in order.
func (s Subject) Params() []string {
	var out []string
	for _, tok := range s.tokens {
		if p, ok := paramOf(tok); ok {
			out = append(out, p)
		}
	}
	return out
}

// ParamTokens lists the token position of each parameter.
func (s Subject) ParamTokens() []int {
	var out []int
	for i, tok := range s.tokens {
		if _, ok := paramOf(tok); ok {
			out = append(out, i)
		}
	}
	return out
}

// Wildcard returns the NATS subject with each parameter replaced by *.
func (s Subject) Wildcard() string {
	out := make([]string, len(s.tokens))
	for i, tok := range s.tokens {
		if _, ok := paramOf(tok); ok {
			tok = "*"
		}
		out[i] = tok
	}
	return strings.Join(out, ".")
}

// HasWildcard reports whether the subject holds a NATS wildcard.
func (s Subject) HasWildcard() bool {
	return slices.Contains(s.tokens, "*") || slices.Contains(s.tokens, ">")
}

// Tokens returns the raw subject tokens.
func (s Subject) Tokens() []string { return s.tokens }

// SubjectsOverlap reports whether any concrete subject matches both patterns.
func SubjectsOverlap(a, b string) bool {
	at, bt := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; ; i++ {
		switch {
		case i == len(at) && i == len(bt):
			return true
		case i == len(at) || i == len(bt):
			return false
		case at[i] == ">" || bt[i] == ">":
			return true
		case at[i] != bt[i] && at[i] != "*" && bt[i] != "*":
			return false
		}
	}
}

// SubjectCovers reports whether pattern matches every subject matching subject.
func SubjectCovers(pattern, subject string) bool {
	pt, st := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, tok := range pt {
		if tok == ">" {
			return i < len(st)
		}
		if i >= len(st) || tok != "*" && tok != st[i] {
			return false
		}
	}
	return len(pt) == len(st)
}
