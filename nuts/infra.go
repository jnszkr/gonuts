package nuts

import "github.com/nats-io/nats.go/jetstream"

// Infra is everything a spec expects to exist on the server.
type Infra struct {
	Streams   []jetstream.StreamConfig   `json:"streams,omitempty"`
	Consumers []ConsumerInfra            `json:"consumers,omitempty"`
	Buckets   []jetstream.KeyValueConfig `json:"buckets,omitempty"`
}

// ConsumerInfra is a consumer and the stream it belongs to.
type ConsumerInfra struct {
	Stream string                   `json:"stream"`
	Config jetstream.ConsumerConfig `json:"config"`
}
