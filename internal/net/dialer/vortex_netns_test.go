//go:build linux && !android

package dialer

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestVortexDialNamespaceRejected(t *testing.T) {
	for _, ns := range []string{"vortex-absent-namespace", "/nonexistent/vortex-namespace"} {
		for _, network := range []string{"tcp", "udp"} {
			t.Run(ns+network, func(t *testing.T) {
				calls := 0
				d := Dialer{Netns: ns, DialFunc: func(context.Context, string, string) (net.Conn, error) {
					calls++
					return nil, errors.New("unexpected dial")
				}}
				conn, err := d.Dial(context.Background(), network, "invalid address")
				if conn != nil {
					conn.Close()
					t.Fatal("connection returned")
				}
				if !errors.Is(err, errors.ErrUnsupported) {
					t.Errorf("namespace error = %v, want unsupported", err)
				}
				if calls != 0 {
					t.Errorf("DialFunc called %d times", calls)
				}
			})
		}
	}
}

func TestVortexSwitchNetnsContract(t *testing.T) {
	restore, err := switchNetns("vortex-absent-namespace")
	if restore != nil || !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("nonempty: cleanup=%v err=%v", restore != nil, err)
	}
	restore, err = switchNetns("")
	if err != nil || restore == nil {
		t.Fatalf("empty: cleanup=%v err=%v", restore != nil, err)
	}
	restore()
}

func TestVortexDialEmptyNamespaceTCPUDP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	accepted := make(chan error, 1)
	go func() {
		conn, err := tcp.Accept()
		if err == nil {
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			_, err = conn.Write([]byte("tcp"))
		}
		accepted <- err
	}()
	d := Dialer{}
	conn, err := d.Dial(ctx, "tcp", tcp.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 3)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "tcp" {
		t.Fatalf("TCP payload %q: %v", buf, err)
	}
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	uconn, err := d.Dial(ctx, "udp", udp.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer uconn.Close()
	udp.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := uconn.Write([]byte("udp")); err != nil {
		t.Fatal(err)
	}
	n, _, err := udp.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "udp" {
		t.Fatalf("UDP payload %q: %v", buf[:n], err)
	}
}
