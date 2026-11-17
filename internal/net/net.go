package net

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"

	xio "github.com/go-gost/x/internal/io"
)

func init() {
	// Disable Multipath TCP by default to mitigate CVE-2026-98432.
	// Listeners opt in explicitly via their "mptcp" metadata.
	if v := os.Getenv("GODEBUG"); !strings.Contains(v, "multipathtcp") {
		if v == "" {
			os.Setenv("GODEBUG", "multipathtcp=0")
		} else {
			os.Setenv("GODEBUG", v+",multipathtcp=0")
		}
	}
}

// SetBuffer is a connection that supports setting send and receive buffer sizes.
type SetBuffer interface {
	SetReadBuffer(bytes int) error
	SetWriteBuffer(bytes int) error
}

// SyscallConn is a connection that exposes its raw system file descriptor.
type SyscallConn interface {
	SyscallConn() (syscall.RawConn, error)
}

// RemoteAddr is a connection that exposes its remote address.
type RemoteAddr interface {
	RemoteAddr() net.Addr
}

// SetDSCP is a connection that supports setting the DSCP field.
type SetDSCP interface {
	SetDSCP(int) error
}

// IsIPv4 reports whether address is an IPv4 address by checking whether
// it starts with ':' or '[' (IPv6) vs a digit (IPv4).
func IsIPv4(address string) bool {
	return address != "" && address[0] != ':' && address[0] != '['
}

// ListenConfig retains the namespace field for compatibility but rejects nonempty values.
type ListenConfig struct {
	Netns string
	net.ListenConfig
}

// Listen rejects namespace switching before creating a listener.
func (lc *ListenConfig) Listen(ctx context.Context, network, address string) (net.Listener, error) {
	if lc.Netns != "" {
		return nil, errors.ErrUnsupported
	}

	return lc.ListenConfig.Listen(ctx, network, address)
}

// ListenPacket rejects namespace switching before creating a packet listener.
func (lc *ListenConfig) ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	if lc.Netns != "" {
		return nil, errors.ErrUnsupported
	}

	return lc.ListenConfig.ListenPacket(ctx, network, address)
}

type readWriteConn struct {
	net.Conn
	r io.Reader
	w io.Writer
}

// NewReadWriteConn returns a net.Conn that reads from r and writes to w,
// delegating all other operations to c. If c supports CloseRead or CloseWrite,
// those are forwarded; otherwise they return ErrUnsupported.
func NewReadWriteConn(r io.Reader, w io.Writer, c net.Conn) net.Conn {
	return &readWriteConn{
		Conn: c,
		r:    r,
		w:    w,
	}
}

func (c *readWriteConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *readWriteConn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

func (c *readWriteConn) CloseRead() error {
	if sc, ok := c.Conn.(xio.CloseRead); ok {
		return sc.CloseRead()
	}
	return xio.ErrUnsupported
}

func (c *readWriteConn) CloseWrite() error {
	if sc, ok := c.Conn.(xio.CloseWrite); ok {
		return sc.CloseWrite()
	}
	return xio.ErrUnsupported
}
