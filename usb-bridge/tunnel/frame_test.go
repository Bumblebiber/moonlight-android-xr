package tunnel

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var wire []byte
	payloads := [][]byte{{}, {1}, bytes.Repeat([]byte{0xAB}, 1408), bytes.Repeat([]byte{7}, MaxPayload)}
	for _, p := range payloads {
		var err error
		if wire, err = AppendFrame(wire, p); err != nil {
			t.Fatal(err)
		}
	}
	r := bytes.NewReader(wire)
	buf := make([]byte, MaxPayload)
	for i, want := range payloads {
		got, err := ReadFrame(r, buf)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d: got %d bytes, want %d", i, len(got), len(want))
		}
	}
	if _, err := ReadFrame(r, buf); err != io.EOF {
		t.Fatalf("after last frame: %v, want EOF", err)
	}
}

func TestFrameOversized(t *testing.T) {
	if _, err := AppendFrame(nil, make([]byte, MaxPayload+1)); err == nil {
		t.Fatal("AppendFrame accepted an oversized payload")
	}
	wire := []byte{0x10, 0x01} // 4097
	wire = append(wire, make([]byte, 4097)...)
	if _, err := ReadFrame(bytes.NewReader(wire), make([]byte, MaxPayload)); err == nil {
		t.Fatal("ReadFrame accepted an oversized length")
	}
}

func TestFrameTruncated(t *testing.T) {
	wire, _ := AppendFrame(nil, []byte{1, 2, 3, 4})
	buf := make([]byte, MaxPayload)
	for cut := 1; cut < len(wire); cut++ {
		_, err := ReadFrame(bytes.NewReader(wire[:cut]), buf)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("cut at %d: %v, want ErrUnexpectedEOF", cut, err)
		}
	}
}

func TestHelloWelcome(t *testing.T) {
	h := Hello{Version: Version, Channel: Control, Session: 0x0102030405060708}
	b := h.Marshal()
	if len(b) != HelloSize {
		t.Fatalf("hello is %d bytes", len(b))
	}
	got, err := ReadHello(bytes.NewReader(b))
	if err != nil || got != h {
		t.Fatalf("hello: %+v %v", got, err)
	}
	w := Welcome{Version: Version, Status: StatusHostUnreachable}
	gw, err := ReadWelcome(bytes.NewReader(w.Marshal()))
	if err != nil || gw != w {
		t.Fatalf("welcome: %+v %v", gw, err)
	}
	b[0] = 'X'
	if _, err := ReadHello(bytes.NewReader(b)); err != ErrBadMagic {
		t.Fatalf("bad magic: %v", err)
	}
	if _, err := ReadHello(bytes.NewReader(b[:5])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short hello: %v", err)
	}
}
