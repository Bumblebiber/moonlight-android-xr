package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Bumblebiber/moonlight-android-xr/usb-bridge/tunnel"
)

// fakeHost echoes every datagram back to its sender, the way Sunshine
// answers whoever pinged, and remembers the senders it saw.
type fakeHost struct {
	conn *net.UDPConn
	mu   sync.Mutex
	from map[string]bool
}

func newFakeHost(t *testing.T) *fakeHost {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	c.SetReadBuffer(4 << 20)
	h := &fakeHost{conn: c, from: map[string]bool{}}
	go func() {
		buf := make([]byte, 65536)
		for {
			n, a, err := c.ReadFromUDP(buf)
			if err != nil {
				return
			}
			h.mu.Lock()
			h.from[a.String()] = true
			h.mu.Unlock()
			c.WriteToUDP(buf[:n], a)
		}
	}()
	t.Cleanup(func() { c.Close() })
	return h
}

func (h *fakeHost) senders() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.from)
}

func startServer(t *testing.T, probe string) (string, *fakeHost) {
	addr, host, _ := startServerSrv(t, probe)
	return addr, host
}

func startServerSrv(t *testing.T, probe string) (string, *fakeHost, *Server) {
	host := newFakeHost(t)
	addr := host.conn.LocalAddr().(*net.UDPAddr)
	srv := &Server{
		UDP:   map[tunnel.Channel]*net.UDPAddr{tunnel.Video: addr, tunnel.Audio: addr, tunnel.Control: addr},
		Probe: probe,
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go srv.Serve(ln)
	return ln.Addr().String(), host, srv
}

func dialTunnel(t *testing.T, addr string, h tunnel.Hello) (net.Conn, tunnel.Status) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.(*net.TCPConn).SetNoDelay(true)
	c.Write(h.Marshal())
	w, err := tunnel.ReadWelcome(c)
	if err != nil {
		t.Fatal(err)
	}
	return c, w.Status
}

func sendFrame(t *testing.T, c net.Conn, p []byte) {
	b, err := tunnel.AppendFrame(nil, p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
}

func TestProbe(t *testing.T) {
	up, _ := net.Listen("tcp", "127.0.0.1:0")
	defer up.Close()
	addr, _ := startServer(t, up.Addr().String())
	c, st := dialTunnel(t, addr, tunnel.Hello{Version: tunnel.Version, Channel: tunnel.Probe})
	c.Close()
	if st != tunnel.StatusOK {
		t.Fatalf("probe with host up: %d", st)
	}

	down, _ := net.Listen("tcp", "127.0.0.1:0")
	downAddr := down.Addr().String()
	down.Close()
	addr, _ = startServer(t, downAddr)
	c, st = dialTunnel(t, addr, tunnel.Hello{Version: tunnel.Version, Channel: tunnel.Probe})
	c.Close()
	if st != tunnel.StatusHostUnreachable {
		t.Fatalf("probe with host down: %d", st)
	}

	c, st = dialTunnel(t, addr, tunnel.Hello{Version: tunnel.Version + 1, Channel: tunnel.Video})
	c.Close()
	if st != tunnel.StatusVersionMismatch {
		t.Fatalf("wrong version: %d", st)
	}
}

// A replug drops the tunnel connection; the headset comes back with the
// same session and must reach Sunshine from the same UDP source, since
// Sunshine keeps sending to the first one it heard from.
func TestReattachKeepsUDPSource(t *testing.T) {
	addr, host := startServer(t, "")
	hello := tunnel.Hello{Version: tunnel.Version, Channel: tunnel.Video, Session: 42}
	buf := make([]byte, tunnel.MaxPayload)

	for round := 0; round < 3; round++ {
		c, st := dialTunnel(t, addr, hello)
		if st != tunnel.StatusOK {
			t.Fatalf("round %d: status %d", round, st)
		}
		msg := []byte{byte(round), 1, 2, 3}
		sendFrame(t, c, msg)
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		got, err := tunnel.ReadFrame(c, buf)
		if err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("round %d: echo %v %v", round, got, err)
		}
		c.Close()
	}
	if n := host.senders(); n != 1 {
		t.Fatalf("host saw %d UDP sources, want 1", n)
	}

	// A new session gets its own socket
	c, _ := dialTunnel(t, addr, tunnel.Hello{Version: tunnel.Version, Channel: tunnel.Video, Session: 43})
	sendFrame(t, c, []byte{9})
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := tunnel.ReadFrame(c, buf); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if n := host.senders(); n != 2 {
		t.Fatalf("host saw %d UDP sources, want 2", n)
	}
}

// Relay video-sized datagrams through the helper to the echo host and back,
// paced like a 150 Mbit/s stream at 60 fps, and report the round trip.
func TestThroughputLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	addr, _, srv := startServerSrv(t, "")
	c, _ := dialTunnel(t, addr, tunnel.Hello{Version: tunnel.Version, Channel: tunnel.Video, Session: 7})
	defer c.Close()

	const pkt, perBurst, bursts = 1408, 222, 120 // 222*1408*8*60 = 150 Mbit/s
	var mu sync.Mutex
	var rtts []time.Duration
	received := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, tunnel.MaxPayload)
		c.SetReadDeadline(time.Now().Add(10 * time.Second))
		for received < perBurst*bursts {
			p, err := tunnel.ReadFrame(c, buf)
			if err != nil {
				return
			}
			received++
			if p[0] == 1 {
				sent := int64(binary.BigEndian.Uint64(p[1:]))
				mu.Lock()
				rtts = append(rtts, time.Duration(time.Now().UnixNano()-sent))
				mu.Unlock()
			}
		}
	}()

	p := make([]byte, pkt)
	start := time.Now()
	tick := time.NewTicker(time.Second / 60)
	for b := 0; b < bursts; b++ {
		<-tick.C
		// Paced the way Sunshine sends a frame: batches of 32 datagrams at
		// about 800 Mbit/s, not one 300 KB lump, which no default UDP buffer
		// survives
		for i := 0; i < perBurst; i += 32 {
			var wire []byte
			for j := i; j < i+32 && j < perBurst; j++ {
				p[0] = 0
				if j == perBurst-1 {
					p[0] = 1
				}
				binary.BigEndian.PutUint64(p[1:], uint64(time.Now().UnixNano()))
				wire, _ = tunnel.AppendFrame(wire, p)
			}
			c.Write(wire)
			time.Sleep(time.Duration(len(wire)*8) * time.Second / 800e6)
		}
	}
	tick.Stop()
	elapsed := time.Since(start)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
	if len(rtts) == 0 {
		t.Fatal("no echoes")
	}
	loss := 1 - float64(received)/float64(perBurst*bursts)
	t.Logf("sent %.0f Mbit/s, received %d/%d (loss %.2f%%), burst-end RTT p50 %v p95 %v max %v",
		float64(perBurst*bursts*pkt*8)/elapsed.Seconds()/1e6, received, perBurst*bursts, loss*100,
		rtts[len(rtts)/2], rtts[len(rtts)*95/100], rtts[len(rtts)-1])
	srv.mu.Lock()
	for _, st := range srv.streams {
		t.Logf("dropped in helper queue: %d", st.dropped.Load())
	}
	srv.mu.Unlock()
	if loss > 0.01 {
		t.Fatalf("loss %.2f%% on loopback", loss*100)
	}
}

func TestParseDevices(t *testing.T) {
	out := "* daemon started successfully\nList of devices attached\n2G0YC5ZG5Q00V1\tdevice\nABC\tunauthorized\n\n"
	ds := parseDevices(out)
	if len(ds) != 2 || ds[0] != (device{"2G0YC5ZG5Q00V1", "device"}) || ds[1].state != "unauthorized" {
		t.Fatalf("%+v", ds)
	}
}
