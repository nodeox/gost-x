package mux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-gost/x/ctx"
	smux "github.com/xtaci/smux"
)

const (
	defaultVersion = 1
)

type Config struct {
	// Type selects "smux" (default); other backends are explicitly rejected.
	Type string

	// SMUX Protocol version, support 1,2
	Version int

	// Disabled keepalive
	KeepAliveDisabled bool

	// KeepAliveInterval is how often to send a NOP command to the remote
	KeepAliveInterval time.Duration

	// KeepAliveTimeout is how long the session
	// will be closed if no data has arrived
	KeepAliveTimeout time.Duration

	// MaxFrameSize is used to control the maximum
	// frame size to sent to the remote
	MaxFrameSize int

	// MaxReceiveBuffer is used to control the maximum
	// number of data in the buffer pool
	MaxReceiveBuffer int

	// MaxStreamBuffer is used to control the maximum
	// number of data per stream
	MaxStreamBuffer int

	// MaxStreamWindow is retained for API compatibility; yamux is unsupported.
	MaxStreamWindow int
}

// Session multiplexes streams over a single underlying connection.
type Session interface {
	GetConn() (net.Conn, error)
	Accept() (net.Conn, error)
	Close() error
	IsClosed() bool
}

func muxType(cfg *Config) string {
	if cfg == nil {
		return "smux"
	}
	if cfg.Type == "" {
		return "smux"
	}
	return cfg.Type
}

// ClientSession creates a client-side mux session over conn.
func ClientSession(conn net.Conn, cfg *Config) (Session, error) {
	if kind := muxType(cfg); kind != "smux" {
		return nil, fmt.Errorf("%w: mux backend %q", errors.ErrUnsupported, kind)
	}
	return newSMUXClientSession(conn, cfg)
}

// ServerSession creates a server-side mux session over conn.
func ServerSession(conn net.Conn, cfg *Config) (Session, error) {
	if kind := muxType(cfg); kind != "smux" {
		return nil, fmt.Errorf("%w: mux backend %q", errors.ErrUnsupported, kind)
	}
	return newSMUXServerSession(conn, cfg)
}

// --- SMUX backend ---

func convertConfig(cfg *Config) *smux.Config {
	smuxCfg := smux.DefaultConfig()
	smuxCfg.Version = defaultVersion

	if cfg == nil {
		return smuxCfg
	}

	if cfg.Version > 0 {
		smuxCfg.Version = cfg.Version
	}
	smuxCfg.KeepAliveDisabled = cfg.KeepAliveDisabled
	if cfg.KeepAliveInterval > 0 {
		smuxCfg.KeepAliveInterval = cfg.KeepAliveInterval
	}
	if cfg.KeepAliveTimeout > 0 {
		smuxCfg.KeepAliveTimeout = cfg.KeepAliveTimeout
	}
	if cfg.MaxFrameSize > 0 {
		smuxCfg.MaxFrameSize = cfg.MaxFrameSize
	}
	if cfg.MaxReceiveBuffer > 0 {
		smuxCfg.MaxReceiveBuffer = cfg.MaxReceiveBuffer
	}
	if cfg.MaxStreamBuffer > 0 {
		smuxCfg.MaxStreamBuffer = cfg.MaxStreamBuffer
	}

	return smuxCfg
}

type smuxSession struct {
	conn    net.Conn
	session *smux.Session
}

func newSMUXClientSession(conn net.Conn, cfg *Config) (Session, error) {
	s, err := smux.Client(conn, convertConfig(cfg))
	if err != nil {
		return nil, err
	}
	return &smuxSession{
		conn:    conn,
		session: s,
	}, nil
}

func newSMUXServerSession(conn net.Conn, cfg *Config) (Session, error) {
	s, err := smux.Server(conn, convertConfig(cfg))
	if err != nil {
		return nil, err
	}
	return &smuxSession{
		conn:    conn,
		session: s,
	}, nil
}

func (session *smuxSession) GetConn() (net.Conn, error) {
	stream, err := session.session.OpenStream()
	if err != nil {
		return nil, err
	}
	return &streamConn{Conn: session.conn, stream: stream}, nil
}

func (session *smuxSession) Accept() (net.Conn, error) {
	stream, err := session.session.AcceptStream()
	if err != nil {
		return nil, err
	}
	return &streamConn{Conn: session.conn, stream: stream}, nil
}

func (session *smuxSession) Close() error {
	if session.session == nil {
		return nil
	}
	return session.session.Close()
}

func (session *smuxSession) IsClosed() bool {
	if session.session == nil {
		return true
	}
	return session.session.IsClosed()
}

type streamConn struct {
	net.Conn
	stream *smux.Stream
}

func (c *streamConn) Read(b []byte) (n int, err error) {
	return c.stream.Read(b)
}

func (c *streamConn) Write(b []byte) (n int, err error) {
	return c.stream.Write(b)
}

func (c *streamConn) Close() error {
	return c.stream.Close()
}

// SetReadDeadline sets the deadline on this stream, never on the session conn
// underneath. The embedded net.Conn is shared by every stream, so a deadline
// set there outlives the stream that set it: net/http parks a read with
// SetReadDeadline(time.Unix(1, 0)) to unblock it on teardown and only clears
// it when a read was actually in flight, leaving the shared conn with a
// deadline in 1970 — and the next session Accept fails with an i/o timeout.
func (c *streamConn) SetReadDeadline(t time.Time) error {
	return c.stream.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline on this stream, never on the session conn
// underneath. See SetReadDeadline.
func (c *streamConn) SetWriteDeadline(t time.Time) error {
	return c.stream.SetWriteDeadline(t)
}

// SetDeadline sets both deadlines on this stream, never on the session conn
// underneath. See SetReadDeadline.
func (c *streamConn) SetDeadline(t time.Time) error {
	return c.stream.SetDeadline(t)
}

func (c *streamConn) Context() context.Context {
	if sc, ok := c.Conn.(ctx.Context); ok {
		return sc.Context()
	}
	return nil
}
