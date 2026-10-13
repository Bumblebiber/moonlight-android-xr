package com.limelight.nvstream.usb;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * The tunnel's frames and handshake, byte for byte what usb-bridge's
 * tunnel/frame.go writes and reads.
 */
public class UsbTunnelTest {

    private static DataInputStream in(byte[] b, int len) {
        return new DataInputStream(new ByteArrayInputStream(b, 0, len));
    }

    @Test
    public void framesRoundTrip() throws IOException {
        byte[][] payloads = {new byte[0], {1}, filled(1408, 0xAB), filled(UsbTunnel.MAX_PAYLOAD, 7)};
        byte[] wire = new byte[4 * (UsbTunnel.MAX_PAYLOAD + 2)];
        int pos = 0;
        for (byte[] p : payloads) {
            pos = UsbTunnel.putFrame(wire, pos, p, 0, p.length);
        }
        DataInputStream in = in(wire, pos);
        byte[] buf = new byte[UsbTunnel.MAX_PAYLOAD];
        for (byte[] p : payloads) {
            int n = UsbTunnel.readFrame(in, buf);
            assertArrayEquals(p, Arrays.copyOf(buf, n));
        }
        try {
            UsbTunnel.readFrame(in, buf);
            fail("read past the last frame");
        } catch (EOFException expected) {
        }
    }

    @Test
    public void frameIsLengthThenBytes() {
        byte[] wire = new byte[5];
        assertEquals(5, UsbTunnel.putFrame(wire, 0, new byte[] {9, 8, 7}, 0, 3));
        assertArrayEquals(new byte[] {0, 3, 9, 8, 7}, wire);
    }

    @Test
    public void oversizedIsRefused() {
        try {
            UsbTunnel.putFrame(new byte[UsbTunnel.MAX_PAYLOAD + 3], 0,
                    new byte[UsbTunnel.MAX_PAYLOAD + 1], 0, UsbTunnel.MAX_PAYLOAD + 1);
            fail("wrote an oversized frame");
        } catch (IllegalArgumentException expected) {
        }
        byte[] wire = new byte[2 + 4097];
        wire[0] = 0x10;
        wire[1] = 0x01;
        try {
            UsbTunnel.readFrame(in(wire, wire.length), new byte[UsbTunnel.MAX_PAYLOAD]);
            fail("read an oversized frame");
        } catch (IOException expected) {
        }
    }

    @Test
    public void truncatedIsAnError() {
        byte[] wire = new byte[6];
        int n = UsbTunnel.putFrame(wire, 0, new byte[] {1, 2, 3, 4}, 0, 4);
        for (int cut = 1; cut < n; cut++) {
            try {
                UsbTunnel.readFrame(in(wire, cut), new byte[UsbTunnel.MAX_PAYLOAD]);
                fail("cut at " + cut + " read as a frame");
            } catch (EOFException e) {
                fail("cut at " + cut + " taken for a clean end");
            } catch (IOException expected) {
            }
        }
    }

    @Test
    public void helloLayout() {
        ByteBuffer b = ByteBuffer.wrap(UsbTunnel.hello(UsbTunnel.CHANNEL_CONTROL, 0x0102030405060708L));
        assertEquals(UsbTunnel.HELLO_SIZE, b.remaining());
        assertEquals('M', b.get());
        assertEquals('L', b.get());
        assertEquals('U', b.get());
        assertEquals('T', b.get());
        assertEquals(UsbTunnel.VERSION, b.get());
        assertEquals(UsbTunnel.CHANNEL_CONTROL, b.get());
        assertEquals(0, b.getShort());
        assertEquals(0x0102030405060708L, b.getLong());
    }

    @Test
    public void welcomeStatus() throws IOException {
        byte[] w = {'M', 'L', 'U', 'T', 1, (byte) UsbTunnel.STATUS_HOST_UNREACHABLE, 0, 0};
        assertEquals(UsbTunnel.STATUS_HOST_UNREACHABLE, UsbTunnel.readWelcome(new ByteArrayInputStream(w)));
        w[0] = 'X';
        try {
            UsbTunnel.readWelcome(new ByteArrayInputStream(w));
            fail("took a bad magic");
        } catch (IOException expected) {
        }
    }

    private static byte[] filled(int n, int v) {
        byte[] b = new byte[n];
        Arrays.fill(b, (byte) v);
        return b;
    }
}
