// Command adbbench measures what an adb reverse TCP connection does under
// stream-like load: the PC side sends 60 bursts per second of 1408-byte
// packets (the size of a Moonlight video datagram), the headset side echoes
// the last packet of every burst, and the PC side reports throughput and the
// round trip of those echoes per rate step.
//
// PC:      adbbench -mode server            (listens on 127.0.0.1:47900)
//
//	adb reverse tcp:47900 tcp:47900
//
// Headset: adb shell /data/local/tmp/adbbench -mode client
//
// With -echo 127.0.0.1:47901 on both sides (and a second adb reverse for
// that port) the echoes come back over their own connection, to tell
// head-of-line blocking in the shared connection apart from other delays.
//
// SPDX-License-Identifier: GPL-3.0-or-later
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pktSize   = 1408
	burstsSec = 60
	typeData  = 1
	typeEcho  = 2 // last packet of a burst, echoed back as its 13-byte header
	hdrSize   = 13
)

func main() {
	mode := flag.String("mode", "", "server (PC) or client (headset)")
	addr := flag.String("addr", "127.0.0.1:47900", "listen/connect address")
	rates := flag.String("rates", "0,50,100,150,300,600", "Mbit/s steps, 0 = idle pings only")
	secs := flag.Int("secs", 10, "seconds per step")
	echo := flag.String("echo", "", "optional separate address for echoes")
	flag.Parse()
	switch *mode {
	case "server":
		server(*addr, *echo, *rates, time.Duration(*secs)*time.Second)
	case "client":
		client(*addr, *echo)
	default:
		flag.Usage()
	}
}

func client(addr, echoAddr string) {
	c := dial(addr)
	e := c
	if echoAddr != "" {
		e = dial(echoAddr)
	}
	buf := make([]byte, pktSize)
	var n int64
	start := time.Now()
	for {
		if _, err := io.ReadFull(c, buf); err != nil {
			fmt.Printf("client: %d MB received in %.1fs, %v\n", n>>20, time.Since(start).Seconds(), err)
			return
		}
		n += pktSize
		if buf[0] == typeEcho {
			if _, err := e.Write(buf[:hdrSize]); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func server(addr, echoAddr, rateList string, step time.Duration) {
	// Both listeners up front: adb reverse accepts on the headset even when
	// nothing listens here yet, and then just drops the connection
	ln := listen(addr)
	var lnEcho net.Listener
	if echoAddr != "" {
		lnEcho = listen(echoAddr)
	}
	fmt.Println("waiting for headset")
	c := accept(ln)
	e := c
	if lnEcho != nil {
		e = accept(lnEcho)
	}

	var mu sync.Mutex
	var rtts []time.Duration
	go func() {
		h := make([]byte, hdrSize)
		for {
			if _, err := io.ReadFull(e, h); err != nil {
				return
			}
			sent := int64(binary.BigEndian.Uint64(h[5:]))
			mu.Lock()
			rtts = append(rtts, time.Duration(time.Now().UnixNano()-sent))
			mu.Unlock()
		}
	}()

	pkt := make([]byte, pktSize)
	var seq uint32
	fmt.Printf("%8s %10s %8s %8s %8s %8s %6s\n", "target", "sent Mb/s", "p50 ms", "p95 ms", "p99 ms", "max ms", "echos")
	for _, f := range strings.Split(rateList, ",") {
		mbit, _ := strconv.Atoi(strings.TrimSpace(f))
		perBurst := 1
		if mbit > 0 {
			perBurst = (mbit*1_000_000/8/burstsSec + pktSize - 1) / pktSize
		}
		mu.Lock()
		rtts = rtts[:0]
		mu.Unlock()
		var sent int64
		t := time.NewTicker(time.Second / burstsSec)
		begin := time.Now()
		for time.Since(begin) < step {
			<-t.C
			for i := 0; i < perBurst; i++ {
				pkt[0] = typeData
				if i == perBurst-1 {
					pkt[0] = typeEcho
				}
				seq++
				binary.BigEndian.PutUint32(pkt[1:], seq)
				binary.BigEndian.PutUint64(pkt[5:], uint64(time.Now().UnixNano()))
				if _, err := c.Write(pkt); err != nil {
					log.Fatal(err)
				}
				sent += pktSize
			}
		}
		t.Stop()
		elapsed := time.Since(begin)
		time.Sleep(500 * time.Millisecond) // let the last echoes arrive
		mu.Lock()
		r := append([]time.Duration(nil), rtts...)
		mu.Unlock()
		fmt.Printf("%8d %10.1f %8s %8s %8s %8s %6d\n", mbit, float64(sent*8)/elapsed.Seconds()/1e6,
			pct(r, 50), pct(r, 95), pct(r, 99), pct(r, 100), len(r))
	}
	c.Close()
}

func dial(addr string) net.Conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	c.(*net.TCPConn).SetNoDelay(true)
	return c
}

func listen(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	return ln
}

func accept(ln net.Listener) net.Conn {
	c, err := ln.Accept()
	if err != nil {
		log.Fatal(err)
	}
	ln.Close()
	c.(*net.TCPConn).SetNoDelay(true)
	fmt.Println("connected from", c.RemoteAddr(), "on", c.LocalAddr())
	return c
}

func pct(d []time.Duration, p int) string {
	if len(d) == 0 {
		return "-"
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	i := (len(d)*p+99)/100 - 1
	if i < 0 {
		i = 0
	}
	return fmt.Sprintf("%.2f", float64(d[i])/float64(time.Millisecond))
}
