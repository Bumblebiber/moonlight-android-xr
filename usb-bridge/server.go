// SPDX-License-Identifier: GPL-3.0-or-later
package main

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bumblebiber/moonlight-android-xr/usb-bridge/tunnel"
)

// sessionGrace is how long a stream's UDP socket outlives its last tunnel
// connection. Sunshine gives up on a client after 10 s without pings, so a
// replug has to land within that anyway.
const sessionGrace = 10 * time.Second

// Queue depths toward the headset, in datagrams. Video gets about two
// frames at 150 Mbit/s and 60 fps; past that, late video is worse than lost
// video, which the stream's FEC and frame invalidation recover from.
var queueDepth = map[tunnel.Channel]int{
	tunnel.Video:   256,
	tunnel.Audio:   64,
	tunnel.Control: 64,
}

// Server accepts tunnel connections from the headset and relays each
// stream to Sunshine over UDP.
type Server struct {
	// Sunshine's UDP port per channel, and a TCP address that answers when
	// Sunshine is up (its HTTP port)
	UDP   map[tunnel.Channel]*net.UDPAddr
	Probe string

	mu      sync.Mutex
	streams map[streamKey]*stream
}

type streamKey struct {
	session uint64
	channel tunnel.Channel
}

// stream is one Moonlight UDP stream: a UDP socket toward Sunshine that
// stays put for the whole session, and the tunnel connection currently
// attached to it, if any.
type stream struct {
	key  streamKey
	udp  *net.UDPConn
	out  chan []byte // datagrams from Sunshine, waiting for the tunnel
	mu   sync.Mutex
	conn net.Conn
	gone time.Time // when conn went away, zero while attached

	dropped atomic.Uint64
}

func (s *Server) Serve(ln net.Listener) error {
	go s.reap()
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	h, err := tunnel.ReadHello(c)
	c.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("tunnel: hello from %v: %v", c.RemoteAddr(), err)
		c.Close()
		return
	}
	status := s.check(h)
	c.Write(tunnel.Welcome{Version: tunnel.Version, Status: status}.Marshal())
	if status != tunnel.StatusOK || h.Channel == tunnel.Probe {
		c.Close()
		return
	}
	st, err := s.attach(h, c)
	if err != nil {
		log.Printf("tunnel: %v", err)
		c.Close()
		return
	}
	s.pump(st, c)
}

func (s *Server) check(h tunnel.Hello) tunnel.Status {
	if h.Version != tunnel.Version {
		return tunnel.StatusVersionMismatch
	}
	if h.Channel != tunnel.Probe && s.UDP[h.Channel] == nil {
		return tunnel.StatusBadChannel
	}
	if h.Channel == tunnel.Probe && s.Probe != "" {
		pc, err := net.DialTimeout("tcp", s.Probe, time.Second)
		if err != nil {
			return tunnel.StatusHostUnreachable
		}
		pc.Close()
	}
	return tunnel.StatusOK
}

// attach hands c the stream for its session and channel, creating the UDP
// socket on first use. A connection coming back after a replug takes over
// from the old one, which is closed.
func (s *Server) attach(h tunnel.Hello, c net.Conn) (*stream, error) {
	key := streamKey{h.Session, h.Channel}
	s.mu.Lock()
	if s.streams == nil {
		s.streams = map[streamKey]*stream{}
	}
	st := s.streams[key]
	if st == nil {
		udp, err := net.DialUDP("udp", nil, s.UDP[h.Channel])
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		// Sunshine sends a frame as a burst of up to 64 KB; Windows' default
		// 64 KB receive buffer drops the tail of it while the reader runs
		udp.SetReadBuffer(4 << 20)
		udp.SetWriteBuffer(1 << 20)
		st = &stream{key: key, udp: udp, out: make(chan []byte, queueDepth[h.Channel])}
		s.streams[key] = st
		go st.fromHost()
		log.Printf("session %016x channel %d: new, udp %v -> %v", h.Session, h.Channel, udp.LocalAddr(), udp.RemoteAddr())
	} else {
		log.Printf("session %016x channel %d: tunnel reattached", h.Session, h.Channel)
	}
	s.mu.Unlock()

	st.mu.Lock()
	old := st.conn
	st.conn, st.gone = c, time.Time{}
	st.mu.Unlock()
	if old != nil {
		old.Close()
	}
	return st, nil
}

// pump runs the attached connection: frames from the headset go out as
// datagrams, queued datagrams from Sunshine go back as frames, several per
// write when they pile up.
func (s *Server) pump(st *stream, c net.Conn) {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64*1024)
		for {
			var p []byte
			select {
			case p = <-st.out:
			case <-stop:
				return
			}
			b, _ := tunnel.AppendFrame(buf[:0], p)
		more:
			for len(b) < cap(buf)-tunnel.MaxPayload-2 {
				select {
				case p = <-st.out:
					b, _ = tunnel.AppendFrame(b, p)
				default:
					break more
				}
			}
			if _, err := c.Write(b); err != nil {
				c.Close()
				return
			}
		}
	}()

	buf := make([]byte, tunnel.MaxPayload)
	for {
		p, err := tunnel.ReadFrame(c, buf)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("session %016x channel %d: tunnel: %v", st.key.session, st.key.channel, err)
			}
			break
		}
		st.udp.Write(p)
	}
	c.Close()
	close(stop)
	<-done
	st.mu.Lock()
	if st.conn == c {
		st.conn, st.gone = nil, time.Now()
	}
	st.mu.Unlock()
}

// fromHost reads Sunshine's datagrams for the life of the stream. With no
// tunnel attached, or the queue full, the datagram is dropped: queueing
// behind a stalled tunnel only turns into latency.
func (st *stream) fromHost() {
	for {
		buf := make([]byte, tunnel.MaxPayload)
		n, err := st.udp.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("session %016x channel %d: udp: %v", st.key.session, st.key.channel, err)
			}
			return
		}
		st.mu.Lock()
		attached := st.conn != nil
		st.mu.Unlock()
		if !attached {
			st.dropped.Add(1)
			continue
		}
		select {
		case st.out <- buf[:n]:
		default:
			st.dropped.Add(1)
		}
	}
}

// reap closes streams whose tunnel has been gone longer than the grace.
func (s *Server) reap() {
	for range time.Tick(time.Second) {
		s.mu.Lock()
		for k, st := range s.streams {
			st.mu.Lock()
			expired := st.conn == nil && !st.gone.IsZero() && time.Since(st.gone) > sessionGrace
			st.mu.Unlock()
			if expired {
				st.udp.Close()
				delete(s.streams, k)
				log.Printf("session %016x channel %d: closed, %d datagrams dropped", k.session, k.channel, st.dropped.Load())
			}
		}
		s.mu.Unlock()
	}
}
