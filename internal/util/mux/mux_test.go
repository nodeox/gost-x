package mux

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// A per-stream deadline must never reach the session conn underneath: the
// session is shared by every stream, so one stream's deadline would decide when
// the others may read. net/http makes this concrete — it parks a pending read
// with SetReadDeadline(time.Unix(1, 0)) to unblock it on teardown, and only
// clears that deadline when a read was actually in flight. A stream conn that
// passes the call through therefore leaves the session deadline in 1970, and
// the next Accept fails with an i/o timeout.
func TestStreamDeadlineStaysOnTheStream(t *testing.T) {
	for _, backend := range []struct {
		name   string
		client func(net.Conn) (Session, error)
		server func(net.Conn) (Session, error)
	}{
		{"smux", func(c net.Conn) (Session, error) { return ClientSession(c, &Config{Version: 2}) },
			func(c net.Conn) (Session, error) { return ServerSession(c, &Config{Version: 2}) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			cli, srv := net.Pipe()
			defer cli.Close()

			srvSession, err := backend.server(srv)
			if err != nil {
				t.Fatal(err)
			}
			defer srvSession.Close()

			cliSession, err := backend.client(cli)
			if err != nil {
				t.Fatal(err)
			}

			// One request served end to end over a stream, then the stream is
			// torn down the way net/http tears one down.
			stream, err := cliSession.GetConn()
			if err != nil {
				t.Fatal("open stream: ", err)
			}

			srvConn, err := srvSession.Accept()
			if err != nil {
				t.Fatal("accept stream: ", err)
			}
			go func() {
				io.WriteString(srvConn, "HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")
				srvConn.Close()
			}()

			req, err := http.NewRequestWithContext(context.Background(), "GET", "http://x/", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := (&http.Client{
				Transport: &http.Transport{
					DialContext: func(context.Context, string, string) (net.Conn, error) { return stream, nil },
				},
			}).Do(req)
			if err != nil {
				t.Fatal("request: ", err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal("read body: ", err)
			}
			if string(body) != "ok" {
				t.Fatalf("body = %q, want %q", body, "ok")
			}

			// net/http's teardown deadline: unblock a pending read by parking
			// it in the past, then clear it only if a read was in flight.
			stream.SetReadDeadline(time.Unix(1, 0))
			stream.SetReadDeadline(time.Time{})

			// The session must still hand out the next stream: its deadline was
			// never touched by the one that just closed.
			done := make(chan error, 1)
			go func() {
				_, err := cliSession.GetConn()
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("session unusable after a served stream: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("session hung after a served stream")
			}
		})
	}
}

type vortexNoIOConn struct {
	net.Conn
	calls atomic.Int32
}

func (c *vortexNoIOConn) Read([]byte) (int, error)         { c.calls.Add(1); return 0, io.EOF }
func (c *vortexNoIOConn) Write([]byte) (int, error)        { c.calls.Add(1); return 0, io.ErrClosedPipe }
func (c *vortexNoIOConn) Close() error                     { c.calls.Add(1); return nil }
func (c *vortexNoIOConn) SetDeadline(time.Time) error      { c.calls.Add(1); return nil }
func (c *vortexNoIOConn) SetReadDeadline(time.Time) error  { c.calls.Add(1); return nil }
func (c *vortexNoIOConn) SetWriteDeadline(time.Time) error { c.calls.Add(1); return nil }

func TestVortexMuxUnsupportedBeforeIO(t *testing.T) {
	for _, kind := range []string{"yamux", "unknown", "SMUX"} {
		for _, constructor := range []struct {
			name string
			make func(net.Conn, *Config) (Session, error)
		}{{"client", ClientSession}, {"server", ServerSession}} {
			t.Run(kind+constructor.name, func(t *testing.T) {
				conn := &vortexNoIOConn{}
				session, err := constructor.make(conn, &Config{Type: kind})
				if session != nil {
					defer session.Close()
				}
				if session != nil || !errors.Is(err, errors.ErrUnsupported) {
					t.Errorf("backend %q session=%v err=%v, want unsupported", kind, session != nil, err)
				}
				if conn.calls.Load() != 0 {
					t.Errorf("rejected constructor touched connection %d times", conn.calls.Load())
				}
			})
		}
	}
}

func vortexMuxRoundTrip(t *testing.T, cfg *Config) {
	t.Helper()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	client, err := ClientSession(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := ServerSession(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	stream, err := client.GetConn()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	peer, err := server.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	stream.SetDeadline(time.Now().Add(3 * time.Second))
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 4)
		_, e := io.ReadFull(peer, buf)
		if e == nil && string(buf) != "ping" {
			e = errors.New("request payload changed")
		}
		if e == nil {
			_, e = peer.Write([]byte("pong"))
		}
		done <- e
	}()
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil || string(buf) != "pong" {
		t.Fatalf("reply %q: %v", buf, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if !client.IsClosed() {
		t.Fatal("closed session still open")
	}
	if next, err := client.GetConn(); err == nil || next != nil {
		if next != nil {
			next.Close()
		}
		t.Fatal("closed session opened stream")
	}
}

func TestVortexMuxNilConfig(t *testing.T) { vortexMuxRoundTrip(t, nil) }
func TestVortexMuxEmptyType(t *testing.T) { vortexMuxRoundTrip(t, &Config{}) }
func TestVortexMuxExplicitSMUX(t *testing.T) {
	vortexMuxRoundTrip(t, &Config{Type: "smux", Version: 1})
}
func TestVortexMuxSMUXVersion2(t *testing.T) {
	vortexMuxRoundTrip(t, &Config{Type: "smux", Version: 2})
}
