// Command usb-bridge carries Moonlight's stream between a headset on USB and
// Sunshine on this PC. It keeps adb reverse rules on every attached headset
// so the headset reaches Sunshine's TCP ports (pairing, RTSP) on its own
// 127.0.0.1, plus the tunnel port, over which the app's relay sends the UDP
// streams (video, audio, control) that adb cannot forward.
//
// SPDX-License-Identifier: GPL-3.0-or-later
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/Bumblebiber/moonlight-android-xr/usb-bridge/tunnel"
)

// TunnelPort is where the app's relay finds the helper on the headset.
const TunnelPort = 47900

// Sunshine's ports as offsets from its base port (47989), the same scheme
// Sunshine and the Moonlight client use.
const (
	offHTTPS   = -5
	offHTTP    = 0
	offVideo   = 9
	offControl = 10
	offAudio   = 11
	offRTSP    = 21
)

func main() {
	adbFlag := flag.String("adb", "", "path to adb (default: $ADB, PATH, then the usual SDK folders)")
	serial := flag.String("s", "", "only serve the device with this serial")
	host := flag.String("host", "127.0.0.1", "address of the PC running Sunshine")
	base := flag.Int("port", 47989, "Sunshine's base port (its `port` setting)")
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime)

	adb, err := findAdb(*adbFlag)
	if err != nil {
		log.Fatal(err)
	}
	hostPort := func(off int) string { return net.JoinHostPort(*host, strconv.Itoa(*base+off)) }
	udp := func(off int) *net.UDPAddr {
		a, err := net.ResolveUDPAddr("udp", hostPort(off))
		if err != nil {
			log.Fatal(err)
		}
		return a
	}

	tcpOffs := []int{offHTTPS, offHTTP, offRTSP}
	rules := []reverseRule{{TunnelPort, TunnelPort}}
	for _, off := range tcpOffs {
		rules = append(rules, reverseRule{*base + off, *base + off})
	}
	// adb reverse only reaches this PC's own ports, so a Sunshine elsewhere
	// is reached through local forwarders on the same ports
	if ip := net.ParseIP(*host); ip == nil || !ip.IsLoopback() {
		for _, off := range tcpOffs {
			go forwardTCP(fmt.Sprintf("127.0.0.1:%d", *base+off), hostPort(off))
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", TunnelPort))
	if err != nil {
		log.Fatalf("tunnel port: %v (is another usb-bridge running?)", err)
	}
	srv := &Server{
		UDP: map[tunnel.Channel]*net.UDPAddr{
			tunnel.Video:   udp(offVideo),
			tunnel.Audio:   udp(offAudio),
			tunnel.Control: udp(offControl),
		},
		Probe: hostPort(offHTTP),
	}
	log.Printf("usb-bridge: adb %s, Sunshine at %s (base port %d)", adb, *host, *base)
	go (&adbWatcher{adb: adb, serial: *serial, rules: rules}).run(context.Background())
	log.Fatal(srv.Serve(ln))
}

func forwardTCP(listen, target string) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Printf("forward %s: %v", listen, err)
		return
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer c.Close()
			t, err := net.Dial("tcp", target)
			if err != nil {
				return
			}
			defer t.Close()
			go io.Copy(t, c)
			io.Copy(c, t)
		}()
	}
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: usb-bridge [flags]\n")
		flag.PrintDefaults()
	}
}
