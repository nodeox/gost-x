package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-gost/core/chain"
	"github.com/go-gost/core/handler"
	"github.com/go-gost/relay"
	xrecorder "github.com/go-gost/x/recorder"
)

type vortexDialCall struct {
	network string
	address string
}

func newVortexConnectHandler(t *testing.T, router *mockRouter) *relayHandler {
	t.Helper()
	h := newInitdHandler(t, handler.RouterOption(router))
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func newVortexConnectRouter(dial func(context.Context, string, string, ...chain.DialOption) (net.Conn, error)) (*mockRouter, chan vortexDialCall) {
	calls := make(chan vortexDialCall, 1)
	return &mockRouter{
		dialFn: func(ctx context.Context, network, address string, opts ...chain.DialOption) (net.Conn, error) {
			calls <- vortexDialCall{network: network, address: address}
			return dial(ctx, network, address, opts...)
		},
	}, calls
}

func expectVortexDial(t *testing.T, calls <-chan vortexDialCall, network, address string) {
	t.Helper()
	select {
	case call := <-calls:
		if call.network != network || call.address != address {
			t.Fatalf("Router.Dial(%q, %q), want (%q, %q)", call.network, call.address, network, address)
		}
	case <-time.After(time.Second):
		t.Fatal("Router.Dial was not called")
	}
}

func startVortexConnect(t *testing.T, h *relayHandler, clientSide, handlerSide, targetSide net.Conn, network, address string) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.handleConnect(ctx, handlerSide, network, address, &xrecorder.HandlerRecorderObject{}, h.options.Logger)
	}()
	t.Cleanup(func() {
		cancel()
		_ = clientSide.Close()
		_ = targetSide.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("handleConnect: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("handleConnect did not stop after connection cleanup")
		}
	})
	return done
}

func responseBytes(t *testing.T, status uint8) []byte {
	t.Helper()
	var buf bytes.Buffer
	resp := relay.Response{Version: relay.Version1, Status: status}
	if _, err := resp.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestVortexConnectTCPFraming(t *testing.T) {
	client, handlerSide := net.Pipe()
	targetSide, targetPeer := net.Pipe()
	for _, conn := range []net.Conn{client, handlerSide, targetSide, targetPeer} {
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	router, calls := newVortexConnectRouter(func(context.Context, string, string, ...chain.DialOption) (net.Conn, error) {
		return targetSide, nil
	})
	h := newVortexConnectHandler(t, router)
	done := startVortexConnect(t, h, client, handlerSide, targetPeer, "tcp", "127.0.0.1:8080")
	expectVortexDial(t, calls, "tcp", "127.0.0.1:8080")

	request := []byte("client-payload")
	clientWrite := make(chan error, 1)
	go func() {
		_, err := client.Write(request)
		clientWrite <- err
	}()
	gotRequest := make([]byte, len(request))
	if _, err := io.ReadFull(targetPeer, gotRequest); err != nil {
		t.Fatal(err)
	}
	if err := <-clientWrite; err != nil {
		t.Fatalf("client write: %v", err)
	}
	if !bytes.Equal(gotRequest, request) {
		t.Fatalf("target payload = %q, want %q", gotRequest, request)
	}

	reply := []byte("target-payload")
	targetWrite := make(chan error, 1)
	go func() {
		_, err := targetPeer.Write(reply)
		targetWrite <- err
	}()
	gotFrame := make([]byte, len(responseBytes(t, relay.StatusOK))+len(reply))
	if _, err := io.ReadFull(client, gotFrame); err != nil {
		t.Fatal(err)
	}
	if err := <-targetWrite; err != nil {
		t.Fatalf("target write: %v", err)
	}
	response := responseBytes(t, relay.StatusOK)
	if !bytes.Equal(gotFrame[:len(response)], response) || !bytes.Equal(gotFrame[len(response):], reply) {
		t.Fatalf("TCP response frame = %x, want header %x followed by %q", gotFrame, response, reply)
	}

	_ = done
}

func TestVortexConnectUDPFraming(t *testing.T) {
	client, handlerSide := net.Pipe()
	targetSide, targetPeer := net.Pipe()
	for _, conn := range []net.Conn{client, handlerSide, targetSide, targetPeer} {
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	router, calls := newVortexConnectRouter(func(context.Context, string, string, ...chain.DialOption) (net.Conn, error) {
		return targetSide, nil
	})
	h := newVortexConnectHandler(t, router)
	h.md.noDelay = true
	done := startVortexConnect(t, h, client, handlerSide, targetPeer, "udp", "127.0.0.1:5353")
	expectVortexDial(t, calls, "udp", "127.0.0.1:5353")

	resp := &relay.Response{}
	if _, err := resp.ReadFrom(client); err != nil {
		t.Fatal(err)
	}
	if resp.Status != relay.StatusOK {
		t.Fatalf("UDP response status = %d, want %d", resp.Status, relay.StatusOK)
	}

	request := []byte("udp-request")
	frame := append([]byte{0, byte(len(request))}, request...)
	clientWrite := make(chan error, 1)
	go func() {
		_, err := client.Write(frame)
		clientWrite <- err
	}()
	gotRequest := make([]byte, len(request))
	if _, err := io.ReadFull(targetPeer, gotRequest); err != nil {
		t.Fatal(err)
	}
	if err := <-clientWrite; err != nil {
		t.Fatalf("client write: %v", err)
	}
	if !bytes.Equal(gotRequest, request) {
		t.Fatalf("target datagram = %q, want %q", gotRequest, request)
	}

	reply := []byte("udp-reply")
	targetWrite := make(chan error, 1)
	go func() {
		_, err := targetPeer.Write(reply)
		targetWrite <- err
	}()
	gotFrame := make([]byte, len(reply)+2)
	if _, err := io.ReadFull(client, gotFrame); err != nil {
		t.Fatal(err)
	}
	if err := <-targetWrite; err != nil {
		t.Fatalf("target write: %v", err)
	}
	if gotFrame[0] != 0 || int(gotFrame[1]) != len(reply) || !bytes.Equal(gotFrame[2:], reply) {
		t.Fatalf("UDP response frame = %x, want [0 %d] followed by %q", gotFrame[:2], len(reply), reply)
	}

	_ = done
}

func assertVortexNetworkRejected(t *testing.T, network string) {
	t.Helper()
	var calls atomic.Int32
	var leakedPeer net.Conn
	router := &mockRouter{
		dialFn: func(context.Context, string, string, ...chain.DialOption) (net.Conn, error) {
			calls.Add(1)
			conn, peer := net.Pipe()
			leakedPeer = peer
			return conn, nil
		},
	}
	h := newVortexConnectHandler(t, router)
	h.md.noDelay = true
	conn := &fakeConn{}
	err := h.handleConnect(context.Background(), conn, network, "127.0.0.1:8080", &xrecorder.HandlerRecorderObject{}, h.options.Logger)
	if leakedPeer != nil {
		_ = leakedPeer.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "unsupported network") {
		t.Fatalf("handleConnect error = %v, want unsupported network", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("Router.Dial calls = %d, want 0", got)
	}
	resp := readRelayResponse(t, conn.writeBuf.Bytes())
	if resp.Status != relay.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", resp.Status, relay.StatusBadRequest)
	}
}

func TestVortexConnectRejectsUnixNetwork(t *testing.T) {
	assertVortexNetworkRejected(t, "unix")
}

func TestVortexConnectRejectsSerialNetwork(t *testing.T) {
	assertVortexNetworkRejected(t, "serial")
}

func TestVortexConnectRejectsUnsupportedNetwork(t *testing.T) {
	assertVortexNetworkRejected(t, "quic")
}

type vortexCloseTrackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *vortexCloseTrackingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestVortexConnectRouterDialFailurePreservesErrorAndClosesPartialConn(t *testing.T) {
	dialErr := errors.New("router dial failed")
	partial, peer := net.Pipe()
	defer peer.Close()
	tracked := &vortexCloseTrackingConn{Conn: partial}
	router := &mockRouter{
		dialFn: func(context.Context, string, string, ...chain.DialOption) (net.Conn, error) {
			return tracked, dialErr
		},
	}
	h := newVortexConnectHandler(t, router)
	h.md.noDelay = true
	conn := &fakeConn{}
	err := h.handleConnect(context.Background(), conn, "tcp", "127.0.0.1:8080", &xrecorder.HandlerRecorderObject{}, h.options.Logger)
	if !errors.Is(err, dialErr) {
		t.Fatalf("handleConnect error = %v, want original dial error %v", err, dialErr)
	}
	if !tracked.closed.Load() {
		t.Fatal("partial Router.Dial connection was not closed")
	}
	resp := readRelayResponse(t, conn.writeBuf.Bytes())
	if resp.Status != relay.StatusNetworkUnreachable {
		t.Fatalf("response status = %d, want %d", resp.Status, relay.StatusNetworkUnreachable)
	}
}
