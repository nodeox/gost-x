package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	yaml "github.com/goccy/go-yaml"
)

// TestSelectorConfigYAML locks in the YAML keys of the selector options.
// Decode only static structure; the GOST configuration-loading API is disabled.
func TestSelectorConfigYAML(t *testing.T) {
	var c Config
	if err := yaml.Unmarshal([]byte(`
hops:
- name: hop0
  selector:
    strategy: parallel
    maxFails: 3
    failTimeout: 15s
    maxParallel: 4
    parallelMode: random
`), &c); err != nil {
		t.Fatalf("read config: %v", err)
	}

	if len(c.Hops) != 1 {
		t.Fatalf("expected 1 hop, got %d", len(c.Hops))
	}

	sel := c.Hops[0].Selector
	if sel == nil {
		t.Fatal("expected a selector config")
	}
	if sel.Strategy != "parallel" {
		t.Errorf("Strategy = %q, want %q", sel.Strategy, "parallel")
	}
	if sel.MaxFails != 3 {
		t.Errorf("MaxFails = %d, want %d", sel.MaxFails, 3)
	}
	if sel.FailTimeout.Seconds() != 15 {
		t.Errorf("FailTimeout = %v, want %v", sel.FailTimeout, 15*1e9)
	}
	if sel.MaxParallel != 4 {
		t.Errorf("MaxParallel = %d, want %d", sel.MaxParallel, 4)
	}
	if sel.ParallelMode != "random" {
		t.Errorf("ParallelMode = %q, want %q", sel.ParallelMode, "random")
	}
}

type vortexCountingReader struct{ reads int }

func (r *vortexCountingReader) Read([]byte) (int, error) {
	r.reads++
	return 0, errors.New("input must not be read")
}

func TestVortexConfigReadRejectedBeforeInput(t *testing.T) {
	for _, format := range []string{"json", "yaml", "hcl", "toml", ""} {
		t.Run(format, func(t *testing.T) {
			c := Config{Nodes: []*NodeConfig{{Name: "unchanged"}}}
			before, _ := json.Marshal(c)
			r := &vortexCountingReader{}
			err := c.Read(r, format)
			if !errors.Is(err, errors.ErrUnsupported) {
				t.Errorf("Read error = %v, want unsupported", err)
			}
			if r.reads != 0 {
				t.Errorf("Read consumed input %d times", r.reads)
			}
			after, _ := json.Marshal(c)
			if !bytes.Equal(before, after) {
				t.Fatal("Read mutated config")
			}
		})
	}
}

func TestVortexConfigExternalLoadRejected(t *testing.T) {
	c := Config{TLS: &TLSConfig{ServerName: "unchanged"}}
	before, _ := json.Marshal(c)
	globalBefore := Global()
	fileBefore := ConfigFile()
	if err := c.Load(); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("Load = %v, want unsupported", err)
	}
	if err := c.ReadFile(filepath.Join(t.TempDir(), "gost.yaml")); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("ReadFile = %v, want unsupported", err)
	}
	after, _ := json.Marshal(c)
	if !bytes.Equal(before, after) || !reflect.DeepEqual(globalBefore, Global()) || fileBefore != ConfigFile() {
		t.Fatal("external loading changed state")
	}
	if ConfigFile() != "" {
		t.Fatal("disabled loader retained config filename")
	}
}

func TestVortexConfigTypedStaticRoundTrip(t *testing.T) {
	c := Config{TLS: &TLSConfig{Secure: true, ServerName: "relay.example", Options: &TLSOptions{MinVersion: "VersionTLS13", ALPN: []string{"vortex"}}}, Nodes: []*NodeConfig{{Name: "exit", Addr: "127.0.0.1:443", TLS: &TLSNodeConfig{Secure: true, ServerName: "exit.example"}}}, Hops: []*HopConfig{{Name: "path", Selector: &SelectorConfig{Strategy: "parallel", MaxFails: 3, FailTimeout: 15 * time.Second, MaxParallel: 4, ParallelMode: "random"}}}}
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var buf bytes.Buffer
			if err := c.Write(&buf, format); err != nil {
				t.Fatal(err)
			}
			var decoded Config
			var err error
			if format == "json" {
				err = json.Unmarshal(buf.Bytes(), &decoded)
			} else {
				err = yaml.Unmarshal(buf.Bytes(), &decoded)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(c)
			after, _ := json.Marshal(decoded)
			if !bytes.Equal(before, after) {
				t.Fatalf("typed config changed after %s roundtrip: %s -> %s", format, before, after)
			}
		})
	}
}

func TestVortexConfigGlobalUpdate(t *testing.T) {
	original := Global()
	defer Set(original)
	Set(&Config{TLS: &TLSConfig{ServerName: "before"}})
	if Global().TLS.ServerName != "before" {
		t.Fatal("Set not visible")
	}
	sentinel := errors.New("update result")
	if err := OnUpdate(func(c *Config) error { c.Nodes = []*NodeConfig{{Name: "typed"}}; return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal("OnUpdate lost callback error")
	}
	if Global().Nodes[0].Name != "typed" {
		t.Fatal("typed update not visible")
	}
}

type vortexFailWriter struct {
	calls int
	err   error
}

func (w *vortexFailWriter) Write([]byte) (int, error) { w.calls++; return 0, w.err }

func TestVortexConfigWriteErrors(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			sentinel := errors.New("writer failed")
			w := &vortexFailWriter{err: sentinel}
			if err := (&Config{}).Write(w, format); !errors.Is(err, sentinel) {
				t.Fatalf("Write = %v, want writer error", err)
			}
		})
	}
}

func TestVortexConfigUnsupportedWriteBeforeOutput(t *testing.T) {
	w := &vortexFailWriter{err: io.ErrClosedPipe}
	if err := (&Config{}).Write(w, "hcl"); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("Write = %v, want unsupported", err)
	}
	if w.calls != 0 {
		t.Fatal("unsupported format touched writer")
	}
}
