package com.limelight.nvstream.usb;

import com.limelight.LimeLog;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The headset end of the USB tunnel. Moonlight streams from 127.0.0.1 in USB
 * mode, so its video, audio and control datagrams arrive here, on Sunshine's
 * own UDP ports on loopback, and go to the usb-bridge helper on the PC
 * through adb reverse, one TCP connection per stream. The TCP side of the
 * stream (pairing, RTSP) needs nothing here: adb reverse maps those ports
 * straight to Sunshine.
 *
 * Has to be started before RTSP SETUP, since Moonlight starts pinging the
 * audio port right after it. A dropped tunnel connection is retried with the
 * same session id, so the helper keeps the same UDP socket toward Sunshine,
 * which only ever sends to the address it heard from first.
 */
public class UsbRelay {
    private static final int RETRY_MS = 250;
    // How often a moving channel logs its traffic, which is what places a
    // stall before or after the headset
    private static final long STATS_NS = 5_000_000_000L;

    private final int tunnelPort;
    private final Channel[] channels;
    private final long session = new SecureRandom().nextLong();
    private volatile boolean stopped;

    /** A relay on Sunshine's default ports. */
    public static UsbRelay forBasePort(int basePort) {
        return new UsbRelay(UsbTunnel.PORT, basePort + 9, basePort + 11, basePort + 10);
    }

    /** Port 0 binds any free port, for tests. */
    public UsbRelay(int tunnelPort, int videoPort, int audioPort, int controlPort) {
        this.tunnelPort = tunnelPort;
        this.channels = new Channel[] {
                new Channel(UsbTunnel.CHANNEL_VIDEO, videoPort),
                new Channel(UsbTunnel.CHANNEL_AUDIO, audioPort),
                new Channel(UsbTunnel.CHANNEL_CONTROL, controlPort),
        };
    }

    /** Binds the UDP ports, then connects the tunnels in the background. */
    public void start() throws IOException {
        try {
            for (Channel c : channels) {
                c.bind();
            }
        } catch (IOException e) {
            stop();
            throw e;
        }
        for (Channel c : channels) {
            c.startThreads();
        }
        LimeLog.info("USB relay: started, session " + Long.toHexString(session));
    }

    public void stop() {
        stopped = true;
        for (Channel c : channels) {
            c.close();
        }
    }

    public long getSession() {
        return session;
    }

    /** The local UDP port of a channel, for tests binding port 0. */
    public int getLocalPort(int channel) {
        return channels[channel - 1].udp.getLocalPort();
    }

    /** The helper's last answer on a channel, or STATUS_NO_HELPER. */
    public int getStatus(int channel) {
        return channels[channel - 1].status;
    }

    private class Channel {
        final int id;
        final int port;
        DatagramSocket udp;
        // Where Moonlight's socket for this stream is, learned from its first
        // datagram; the PC's answers go there
        volatile SocketAddress client;
        volatile Socket tunnel;
        volatile OutputStream tunnelOut;
        volatile int status = UsbTunnel.STATUS_NO_HELPER;
        final AtomicLong upCount = new AtomicLong();

        Channel(int id, int port) {
            this.id = id;
            this.port = port;
        }

        void bind() throws IOException {
            udp = new DatagramSocket(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port));
            udp.setReceiveBufferSize(1 << 20);
        }

        void startThreads() {
            Thread up = new Thread(this::toPc, "USB relay up " + id);
            Thread down = new Thread(this::fromPc, "USB relay down " + id);
            up.setDaemon(true);
            down.setDaemon(true);
            up.start();
            down.start();
        }

        // Moonlight to PC. With no tunnel up the datagram is dropped; the
        // stream retries what matters (pings, ENet).
        private void toPc() {
            byte[] buf = new byte[UsbTunnel.MAX_PAYLOAD];
            byte[] frame = new byte[UsbTunnel.MAX_PAYLOAD + 2];
            DatagramPacket pkt = new DatagramPacket(buf, buf.length);
            while (!stopped) {
                try {
                    pkt.setLength(buf.length);
                    udp.receive(pkt);
                } catch (IOException e) {
                    return;
                }
                client = pkt.getSocketAddress();
                OutputStream out = tunnelOut;
                if (out == null) {
                    continue;
                }
                int n = UsbTunnel.putFrame(frame, 0, buf, 0, pkt.getLength());
                try {
                    out.write(frame, 0, n);
                    upCount.incrementAndGet();
                } catch (IOException e) {
                    closeTunnel();
                }
            }
        }

        // PC to Moonlight, reconnecting the tunnel as long as the relay runs
        private void fromPc() {
            byte[] buf = new byte[UsbTunnel.MAX_PAYLOAD];
            boolean wasUp = false;
            while (!stopped) {
                try {
                    Socket s = new Socket();
                    s.setTcpNoDelay(true);
                    s.connect(new InetSocketAddress("127.0.0.1", tunnelPort), 1000);
                    tunnel = s;
                    s.getOutputStream().write(UsbTunnel.hello(id, session));
                    DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 64 * 1024));
                    status = UsbTunnel.readWelcome(in);
                    if (status != UsbTunnel.STATUS_OK) {
                        throw new IOException("helper said " + status);
                    }
                    tunnelOut = s.getOutputStream();
                    LimeLog.info("USB relay: channel " + id + (wasUp ? " reconnected" : " connected"));
                    wasUp = true;
                    long frames = 0, bytes = 0, maxGap = 0, sendErrors = 0;
                    long last = System.nanoTime(), statsStart = last;
                    for (;;) {
                        int n = UsbTunnel.readFrame(in, buf);
                        long now = System.nanoTime();
                        maxGap = Math.max(maxGap, now - last);
                        last = now;
                        frames++;
                        bytes += n;
                        SocketAddress to = client;
                        if (to != null) {
                            // A failed send loses this datagram, not the tunnel
                            try {
                                udp.send(new DatagramPacket(buf, n, to));
                            } catch (IOException e) {
                                sendErrors++;
                            }
                        }
                        if (now - statsStart >= STATS_NS) {
                            double secs = (now - statsStart) / 1e9;
                            LimeLog.info(String.format(Locale.ROOT,
                                    "USB relay: channel %d %.0fs: down %d (%.1f Mbit/s), longest gap %.1f ms, send errors %d, up %d",
                                    id, secs, frames, bytes * 8 / secs / 1e6, maxGap / 1e6, sendErrors,
                                    upCount.getAndSet(0)));
                            frames = bytes = maxGap = sendErrors = 0;
                            statsStart = now;
                        }
                    }
                } catch (IOException e) {
                    if (wasUp && tunnelOut != null && !stopped) {
                        LimeLog.warning("USB relay: channel " + id + " lost: " + e.getMessage());
                    }
                    closeTunnel();
                    if (status == UsbTunnel.STATUS_OK) {
                        status = UsbTunnel.STATUS_NO_HELPER;
                    }
                }
                try {
                    Thread.sleep(RETRY_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        void closeTunnel() {
            tunnelOut = null;
            Socket s = tunnel;
            if (s != null) {
                try {
                    s.close();
                } catch (IOException ignored) {
                }
            }
        }

        void close() {
            closeTunnel();
            if (udp != null) {
                udp.close();
            }
        }
    }
}
