// Package tunnel is the wire format between the headset's relay and the PC
// helper. Each Moonlight UDP stream (video, audio, control) gets its own TCP
// connection through adb reverse, because small headset-to-PC packets stuck
// behind bulk video in one shared connection cost about a burst interval
// (~17 ms at p95, measured with adbbench).
//
// A connection starts with a Hello from the headset and a Welcome from the
// helper, then carries datagrams as frames: a big-endian u16 length followed
// by that many bytes, one UDP datagram per frame, both directions.
//
// SPDX-License-Identifier: GPL-3.0-or-later
package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Version = 1
	// Moonlight's largest datagram is a 1408-byte video packet and ENet's
	// MTU tops out at 4096; anything longer is a broken stream
	MaxPayload = 4096

	HelloSize   = 16
	WelcomeSize = 8
)

var magic = [4]byte{'M', 'L', 'U', 'T'}

// Channel says what a connection carries. Probe connections only ask the
// helper for its status and close.
type Channel uint8

const (
	Probe   Channel = 0
	Video   Channel = 1
	Audio   Channel = 2
	Control Channel = 3
)

// Status is the helper's answer in the Welcome.
type Status uint8

const (
	StatusOK              Status = 0
	StatusVersionMismatch Status = 1
	StatusHostUnreachable Status = 2 // Sunshine not answering on the PC
	StatusBadChannel      Status = 3
)

// Hello opens a connection. Session ties the three stream connections of
// one stream together, and lets a connection that comes back after a
// replug pick up the same UDP socket toward Sunshine, which only ever sends
// to the address it first heard from.
type Hello struct {
	Version uint8
	Channel Channel
	Session uint64
}

type Welcome struct {
	Version uint8
	Status  Status
}

var ErrBadMagic = errors.New("tunnel: bad magic")

func (h Hello) Marshal() []byte {
	b := make([]byte, HelloSize)
	copy(b, magic[:])
	b[4] = h.Version
	b[5] = byte(h.Channel)
	binary.BigEndian.PutUint64(b[8:], h.Session)
	return b
}

func ReadHello(r io.Reader) (Hello, error) {
	var b [HelloSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Hello{}, err
	}
	if [4]byte(b[:4]) != magic {
		return Hello{}, ErrBadMagic
	}
	return Hello{Version: b[4], Channel: Channel(b[5]), Session: binary.BigEndian.Uint64(b[8:])}, nil
}

func (w Welcome) Marshal() []byte {
	b := make([]byte, WelcomeSize)
	copy(b, magic[:])
	b[4] = w.Version
	b[5] = byte(w.Status)
	return b
}

func ReadWelcome(r io.Reader) (Welcome, error) {
	var b [WelcomeSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Welcome{}, err
	}
	if [4]byte(b[:4]) != magic {
		return Welcome{}, ErrBadMagic
	}
	return Welcome{Version: b[4], Status: Status(b[5])}, nil
}

// AppendFrame appends p as one frame to dst. Callers batch several frames
// into one write.
func AppendFrame(dst, p []byte) ([]byte, error) {
	if len(p) > MaxPayload {
		return dst, fmt.Errorf("tunnel: payload %d bytes exceeds %d", len(p), MaxPayload)
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(p)))
	return append(dst, p...), nil
}

// ReadFrame reads one frame into buf, which must hold MaxPayload bytes, and
// returns the payload. A length over MaxPayload means the stream is out of
// step and cannot be resynchronised, so the connection has to go.
func ReadFrame(r io.Reader, buf []byte) ([]byte, error) {
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(h[:]))
	if n > MaxPayload {
		return nil, fmt.Errorf("tunnel: frame of %d bytes exceeds %d", n, MaxPayload)
	}
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf[:n], nil
}
