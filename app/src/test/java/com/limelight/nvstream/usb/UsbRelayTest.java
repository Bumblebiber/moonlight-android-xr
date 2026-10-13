package com.limelight.nvstream.usb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The relay against a fake usb-bridge that echoes every frame back: what
 * Moonlight sends to a stream port comes back from it, a dropped tunnel
 * comes back under the same session, and the round trip holds up at a
 * stream's rate.
 */
public class UsbRelayTest {

    private ServerSocket helper;
    private final List<Socket> conns = new CopyOnWriteArrayList<>();
    private final List<Long> sessions = new CopyOnWriteArrayList<>();
    private UsbRelay relay;
    private DatagramSocket moonlight;

    @Before
    public void setUp() throws IOException {
        helper = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        Thread t = new Thread(this::acceptLoop);
        t.setDaemon(true);
        t.start();
        relay = new UsbRelay(helper.getLocalPort(), 0, 0, 0);
        relay.start();
        moonlight = new DatagramSocket(0, InetAddress.getByName("127.0.0.1"));
        moonlight.setSoTimeout(200);
    }

    @After
    public void tearDown() throws IOException {
        relay.stop();
        helper.close();
        moonlight.close();
    }

    private void acceptLoop() {
        while (true) {
            Socket s;
            try {
                s = helper.accept();
            } catch (IOException e) {
                return;
            }
            conns.add(s);
            new Thread(() -> echo(s)).start();
        }
    }

    private void echo(Socket s) {
        try {
            s.setTcpNoDelay(true);
            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();
            byte[] hello = new byte[UsbTunnel.HELLO_SIZE];
            new DataInputStream(in).readFully(hello);
            sessions.add(ByteBuffer.wrap(hello, 8, 8).getLong());
            out.write(new byte[] {'M', 'L', 'U', 'T', UsbTunnel.VERSION, UsbTunnel.STATUS_OK, 0, 0});
            DataInputStream din = new DataInputStream(new BufferedInputStream(in));
            byte[] buf = new byte[UsbTunnel.MAX_PAYLOAD];
            byte[] frame = new byte[UsbTunnel.MAX_PAYLOAD + 2];
            while (true) {
                int n = UsbTunnel.readFrame(din, buf);
                out.write(frame, 0, UsbTunnel.putFrame(frame, 0, buf, 0, n));
            }
        } catch (IOException ignored) {
        }
    }

    /** Sends until an answer comes back, since the tunnel connects in the background. */
    private byte[] roundTrip(int channel, byte[] msg) throws IOException {
        DatagramPacket out = new DatagramPacket(msg, msg.length,
                InetAddress.getByName("127.0.0.1"), relay.getLocalPort(channel));
        byte[] buf = new byte[UsbTunnel.MAX_PAYLOAD];
        DatagramPacket in = new DatagramPacket(buf, buf.length);
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            moonlight.send(out);
            try {
                // Earlier tries may still echo in late; only this message counts
                while (true) {
                    in.setLength(buf.length);
                    moonlight.receive(in);
                    byte[] got = Arrays.copyOf(buf, in.getLength());
                    if (Arrays.equals(got, msg)) {
                        return got;
                    }
                }
            } catch (SocketTimeoutException ignored) {
            }
        }
        throw new AssertionError("no answer on channel " + channel);
    }

    @Test
    public void everyChannelRoundTrips() throws IOException {
        for (int ch = UsbTunnel.CHANNEL_VIDEO; ch <= UsbTunnel.CHANNEL_CONTROL; ch++) {
            byte[] msg = {(byte) ch, 1, 2, 3};
            assertArrayEquals(msg, roundTrip(ch, msg));
            assertEquals(UsbTunnel.STATUS_OK, relay.getStatus(ch));
        }
        assertEquals(3, sessions.size());
        for (long s : sessions) {
            assertEquals(relay.getSession(), s);
        }
    }

    @Test
    public void reconnectsWithTheSameSession() throws Exception {
        byte[] msg = {42};
        assertArrayEquals(msg, roundTrip(UsbTunnel.CHANNEL_VIDEO, msg));
        int before = conns.size();
        // A replug: the helper side of every tunnel goes away
        for (Socket s : conns) {
            s.close();
        }
        assertArrayEquals(msg, roundTrip(UsbTunnel.CHANNEL_VIDEO, msg));
        assertTrue("no new tunnel connection", conns.size() > before);
        for (long s : sessions) {
            assertEquals(relay.getSession(), s);
        }
    }

    /** About 150 Mbit/s of 1408-byte datagrams, reporting the round trip. */
    @Test
    public void holdsAStreamRate() throws Exception {
        roundTrip(UsbTunnel.CHANNEL_VIDEO, new byte[] {0});
        double loss = streamRate(moonlight, relay.getLocalPort(UsbTunnel.CHANNEL_VIDEO), "USB relay loopback");
        assertTrue("loss " + loss, loss < 0.02);
    }

    /**
     * Sends 60 bursts of 150 Mbit/s worth of 1408-byte datagrams to port,
     * paced like Sunshine (batches of 32 at about 800 Mbit/s), expects them
     * echoed back, prints the round trip of each burst's last datagram and
     * returns the loss.
     */
    static double streamRate(DatagramSocket sock, int port, String label) throws Exception {
        sock.setReceiveBufferSize(4 << 20);
        final int pkt = 1408, perBurst = 222, bursts = 60;
        byte[] p = new byte[pkt];
        DatagramPacket out = new DatagramPacket(p, pkt, InetAddress.getByName("127.0.0.1"), port);
        List<Long> rtts = Collections.synchronizedList(new ArrayList<>());
        int[] received = {0};
        Thread rx = new Thread(() -> {
            byte[] buf = new byte[UsbTunnel.MAX_PAYLOAD];
            DatagramPacket in = new DatagramPacket(buf, buf.length);
            try {
                sock.setSoTimeout(1000);
                while (received[0] < perBurst * bursts) {
                    in.setLength(buf.length);
                    sock.receive(in);
                    received[0]++;
                    if (buf[0] == 1) {
                        rtts.add(System.nanoTime() - ByteBuffer.wrap(buf, 1, 8).getLong());
                    }
                }
            } catch (IOException ignored) {
            }
        });
        rx.start();
        long start = System.nanoTime();
        for (int b = 0; b < bursts; b++) {
            long due = start + b * 1_000_000_000L / 60;
            while (System.nanoTime() < due) {
                Thread.sleep(0, 200_000);
            }
            for (int i = 0; i < perBurst; i++) {
                p[0] = (byte) (i == perBurst - 1 ? 1 : 0);
                ByteBuffer.wrap(p, 1, 8).putLong(System.nanoTime());
                sock.send(out);
                if (i % 32 == 31) {
                    Thread.sleep(0, 450_000);
                }
            }
        }
        rx.join(5000);
        Collections.sort(rtts);
        double loss = 1 - received[0] / (double) (perBurst * bursts);
        System.out.printf("%s: %d/%d received (loss %.2f%%), burst-end RTT p50 %.2f ms p95 %.2f ms max %.2f ms%n",
                label, received[0], perBurst * bursts, loss * 100,
                rtts.isEmpty() ? -1 : rtts.get(rtts.size() / 2) / 1e6,
                rtts.isEmpty() ? -1 : rtts.get(rtts.size() * 95 / 100) / 1e6,
                rtts.isEmpty() ? -1 : rtts.get(rtts.size() - 1) / 1e6);
        return loss;
    }
}
