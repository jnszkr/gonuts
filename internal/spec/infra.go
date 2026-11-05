package spec

import (
	"time"

	"github.com/jnszkr/gonuts/nuts"
	"github.com/nats-io/nats.go/jetstream"
)

// Infra converts the spec into the server configuration it describes.
func (s *Spec) Infra() nuts.Infra {
	var out nuts.Infra
	for _, name := range SortedKeys(s.Streams) {
		out.Streams = append(out.Streams, s.streamConfig(name))
	}
	for _, name := range SortedKeys(s.Consumers) {
		out.Consumers = append(out.Consumers, nuts.ConsumerInfra{
			Stream: s.Consumers[name].Stream,
			Config: s.consumerConfig(name),
		})
	}
	for _, name := range SortedKeys(s.KV) {
		out.Buckets = append(out.Buckets, bucketConfig(name, s.KV[name]))
	}
	return out
}

func (s *Spec) streamConfig(name string) jetstream.StreamConfig {
	st := s.Streams[name]
	cfg := jetstream.StreamConfig{
		Name:                   name,
		Description:            st.Description,
		Subjects:               s.StreamSubjects(name),
		Retention:              retentionPolicies[st.Retention],
		MaxConsumers:           st.MaxConsumers,
		MaxMsgs:                st.MaxMsgs,
		MaxBytes:               st.MaxBytes,
		Discard:                discardPolicies[st.Discard],
		DiscardNewPerSubject:   st.DiscardNewPerSubject,
		MaxAge:                 st.MaxAge.Duration,
		MaxMsgsPerSubject:      st.MaxMsgsPerSubject,
		MaxMsgSize:             st.MaxMsgSize,
		Storage:                storageTypes[st.Storage],
		Replicas:               st.Replicas,
		NoAck:                  st.NoAck,
		Duplicates:             st.Duplicates.Duration,
		Placement:              placement(st.Placement),
		Mirror:                 source(st.Mirror),
		Sources:                sources(st.Sources),
		Sealed:                 st.Sealed,
		DenyDelete:             st.DenyDelete,
		DenyPurge:              st.DenyPurge,
		AllowRollup:            st.AllowRollup || st.AllowMsgSchedules,
		Compression:            compressions[st.Compression],
		FirstSeq:               st.FirstSeq,
		RePublish:              republish(st.RePublish),
		AllowDirect:            st.AllowDirect,
		MirrorDirect:           st.MirrorDirect,
		Metadata:               st.Metadata,
		AllowMsgTTL:            st.AllowMsgTTL,
		SubjectDeleteMarkerTTL: st.SubjectDeleteMarkerTTL.Duration,
		AllowMsgCounter:        st.AllowMsgCounter,
		AllowAtomicPublish:     st.AllowAtomic,
		AllowMsgSchedules:      st.AllowMsgSchedules,
		PersistMode:            persistModes[st.PersistMode],
		AllowBatchPublish:      st.AllowBatched,
	}
	if t := st.SubjectTransform; t != nil {
		cfg.SubjectTransform = &jetstream.SubjectTransformConfig{Source: t.Source, Destination: t.Destination}
	}
	if l := st.ConsumerLimits; l != nil {
		cfg.ConsumerLimits = jetstream.StreamConsumerLimits{
			InactiveThreshold: l.InactiveThreshold.Duration,
			MaxAckPending:     l.MaxAckPending,
		}
	}
	return cfg
}

func (s *Spec) consumerConfig(name string) jetstream.ConsumerConfig {
	c := s.Consumers[name]
	cfg := jetstream.ConsumerConfig{
		Durable:            name,
		Description:        c.Description,
		DeliverPolicy:      deliverPolicies[c.DeliverPolicy],
		OptStartSeq:        c.OptStartSeq,
		OptStartTime:       c.OptStartTime,
		AckPolicy:          ackPolicies[c.AckPolicy],
		AckWait:            c.AckWait.Duration,
		MaxDeliver:         c.MaxDeliver,
		BackOff:            durations(c.BackOff),
		ReplayPolicy:       replayPolicies[c.ReplayPolicy],
		RateLimit:          c.RateLimit,
		SampleFrequency:    c.SampleFreq,
		MaxWaiting:         c.MaxWaiting,
		MaxAckPending:      c.MaxAckPending,
		HeadersOnly:        c.HeadersOnly,
		MaxRequestBatch:    c.MaxBatch,
		MaxRequestExpires:  c.MaxExpires.Duration,
		MaxRequestMaxBytes: c.MaxBytes,
		InactiveThreshold:  c.InactiveThreshold.Duration,
		Replicas:           c.Replicas,
		MemoryStorage:      c.MemoryStorage,
		Metadata:           c.Metadata,
		PauseUntil:         c.PauseUntil,
		PriorityPolicy:     priorityPolicies[c.PriorityPolicy],
		PinnedTTL:          c.PriorityTimeout.Duration,
		PriorityGroups:     c.PriorityGroups,
		DeliverSubject:     c.DeliverSubject,
		DeliverGroup:       c.DeliverGroup,
		FlowControl:        c.FlowControl,
		IdleHeartbeat:      c.IdleHeartbeat.Duration,
	}
	filters := s.ConsumerFilters(name)
	if len(filters) == 1 {
		cfg.FilterSubject = filters[0]
	} else {
		cfg.FilterSubjects = filters
	}
	return cfg
}

// ConsumerFilters returns the subjects a consumer reads: its events' and explicit ones.
func (s *Spec) ConsumerFilters(name string) []string {
	c := s.Consumers[name]
	var filters []string
	for _, e := range c.Events {
		filters = append(filters, s.EventFilter(e))
	}
	if c.FilterSubject != "" {
		filters = append(filters, c.FilterSubject)
	}
	return append(filters, c.FilterSubjects...)
}

func bucketConfig(name string, k KVBucket) jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:         name,
		Description:    k.Description,
		MaxValueSize:   k.MaxValueSize,
		History:        k.History,
		TTL:            k.TTL.Duration,
		MaxBytes:       k.MaxBytes,
		Storage:        storageTypes[k.Storage],
		Replicas:       k.Replicas,
		Placement:      placement(k.Placement),
		RePublish:      republish(k.RePublish),
		Mirror:         source(k.Mirror),
		Sources:        sources(k.Sources),
		Compression:    k.Compression,
		LimitMarkerTTL: k.LimitMarkerTTL.Duration,
		Metadata:       k.Metadata,
	}
}

// placement leaves out Preferred, which nats.go has no field for. Terraform sets
// it on streams and reports it as unsupported on buckets, so only devnats misses
// it. Verify does not compare placement.
func placement(p *Placement) *jetstream.Placement {
	if p == nil {
		return nil
	}
	return &jetstream.Placement{Cluster: p.Cluster, Tags: p.Tags}
}

func republish(r *RePublish) *jetstream.RePublish {
	if r == nil {
		return nil
	}
	return &jetstream.RePublish{Source: r.Source, Destination: r.Destination, HeadersOnly: r.HeadersOnly}
}

func source(x *StreamSource) *jetstream.StreamSource {
	if x == nil {
		return nil
	}
	return &jetstream.StreamSource{Name: x.Name, FilterSubject: x.FilterSubject, OptStartSeq: x.StartSeq, OptStartTime: x.StartTime}
}

func sources(xs []StreamSource) []*jetstream.StreamSource {
	var out []*jetstream.StreamSource
	for i := range xs {
		out = append(out, source(&xs[i]))
	}
	return out
}

func durations(ds []Duration) []time.Duration {
	var out []time.Duration
	for _, d := range ds {
		out = append(out, d.Duration)
	}
	return out
}

var (
	retentionPolicies = map[string]jetstream.RetentionPolicy{"limits": jetstream.LimitsPolicy, "interest": jetstream.InterestPolicy, "workqueue": jetstream.WorkQueuePolicy}
	discardPolicies   = map[string]jetstream.DiscardPolicy{"old": jetstream.DiscardOld, "new": jetstream.DiscardNew}
	storageTypes      = map[string]jetstream.StorageType{"file": jetstream.FileStorage, "memory": jetstream.MemoryStorage}
	compressions      = map[string]jetstream.StoreCompression{"none": jetstream.NoCompression, "s2": jetstream.S2Compression}
	persistModes      = map[string]jetstream.PersistModeType{"default": jetstream.DefaultPersistMode, "async": jetstream.AsyncPersistMode}
	ackPolicies       = map[string]jetstream.AckPolicy{"explicit": jetstream.AckExplicitPolicy, "all": jetstream.AckAllPolicy, "none": jetstream.AckNonePolicy}
	deliverPolicies   = map[string]jetstream.DeliverPolicy{
		"all": jetstream.DeliverAllPolicy, "last": jetstream.DeliverLastPolicy, "new": jetstream.DeliverNewPolicy,
		"by_start_sequence": jetstream.DeliverByStartSequencePolicy, "by_start_time": jetstream.DeliverByStartTimePolicy,
		"last_per_subject": jetstream.DeliverLastPerSubjectPolicy,
	}
	replayPolicies   = map[string]jetstream.ReplayPolicy{"instant": jetstream.ReplayInstantPolicy, "original": jetstream.ReplayOriginalPolicy}
	priorityPolicies = map[string]jetstream.PriorityPolicy{
		"none": jetstream.PriorityPolicyNone, "pinned_client": jetstream.PriorityPolicyPinned,
		"overflow": jetstream.PriorityPolicyOverflow, "prioritized": jetstream.PriorityPolicyPrioritized,
	}
)
