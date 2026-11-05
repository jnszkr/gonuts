package nuts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// Reply codes that are not declared in a spec.
const (
	CodeInternal   = "500"
	CodeBadRequest = "400"
)

// ReplyError is an error reply from a server.
type ReplyError struct {
	Code    string
	Message string
}

func (e *ReplyError) Error() string { return e.Code + ": " + e.Message }

// CodedError is implemented by the typed errors declared in a spec.
type CodedError interface {
	error
	ReplyCode() string
	ReplyMessage() string
}

// Request sends a request and waits for its reply. The spec's timeout is the
// default, and a shorter deadline on ctx wins.
func Request[Req, Resp any](ctx context.Context, r *Runtime, name, subject string, timeout time.Duration, req Req) (*Resp, error) {
	data, err := encode(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var resp *Resp
	c := &Call{Kind: KindRequest, Name: name, Subject: subject}
	err = r.do(ctx, c, func(ctx context.Context) error {
		reply, err := r.nc.RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Data: data, Header: c.Headers})
		if err != nil {
			return err
		}
		if code := reply.Header.Get(micro.ErrorCodeHeader); code != "" {
			return &ReplyError{Code: code, Message: reply.Header.Get(micro.ErrorHeader)}
		}
		resp = new(Resp)
		if err := json.Unmarshal(reply.Data, resp); err != nil {
			return fmt.Errorf("nuts: decode reply for %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ServerConfig describes one request group's server.
type ServerConfig struct {
	Name       string
	Version    string
	QueueGroup string
}

// Server serves request-reply endpoints using the NATS micro framework, which
// provides discovery, stats and queue groups.
type Server struct {
	ctx  context.Context //nolint:containedctx // handlers outlive the call that registered them
	rt   *Runtime
	name string
	svc  micro.Service
}

// NewServer registers a service. ctx is passed to every handler.
func NewServer(ctx context.Context, r *Runtime, cfg ServerConfig) (*Server, error) {
	version := cfg.Version
	if version == "" {
		version = "1.0.0"
	}
	svc, err := micro.AddService(r.nc, micro.Config{Name: cfg.Name, Version: version, QueueGroup: cfg.QueueGroup})
	if err != nil {
		return nil, fmt.Errorf("nuts: add service %s: %w", cfg.Name, err)
	}
	return &Server{ctx: ctx, rt: r, name: cfg.Name, svc: svc}, nil
}

// Stop drains the service.
func (s *Server) Stop() error {
	return s.svc.Stop()
}

// AddEndpoint registers a typed handler on a subject.
func AddEndpoint[Req, Resp any](s *Server, name, subject string, h func(context.Context, Req) (*Resp, error)) error {
	full := s.name + "." + name
	handler := micro.HandlerFunc(func(mr micro.Request) {
		var body []byte
		c := &Call{Kind: KindServe, Name: full, Subject: mr.Subject(), Headers: nats.Header(mr.Headers())}
		err := s.rt.do(s.ctx, c, func(ctx context.Context) error {
			return recovered(func() error {
				var req Req
				if err := json.Unmarshal(mr.Data(), &req); err != nil {
					return &ReplyError{Code: CodeBadRequest, Message: "invalid request payload"}
				}
				resp, err := h(ctx, req)
				if err != nil {
					return err
				}
				body, err = encode(resp)
				return err
			})
		})
		respond(mr, body, err)
	})
	if err := s.svc.AddEndpoint(name, handler, micro.WithEndpointSubject(subject)); err != nil {
		return fmt.Errorf("nuts: add endpoint %s: %w", full, err)
	}
	return nil
}

func respond(mr micro.Request, body []byte, err error) {
	if err == nil {
		_ = mr.Respond(body)
		return
	}
	code, message := CodeInternal, "internal error"
	var coded CodedError
	var reply *ReplyError
	switch {
	case errors.As(err, &coded):
		code, message = coded.ReplyCode(), coded.ReplyMessage()
	case errors.As(err, &reply):
		code, message = reply.Code, reply.Message
	}
	_ = mr.Error(code, message, nil)
}
