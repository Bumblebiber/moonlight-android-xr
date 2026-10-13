package com.limelight.nvstream.usb;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * The wire format between this app's relay and the usb-bridge helper on the
 * PC, mirrored in usb-bridge/tunnel/frame.go. Each Moonlight UDP stream gets
 * its own TCP connection through adb reverse to 127.0.0.1:47900. A
 * connection opens with a Hello from here and a Welcome from the helper,
 * then carries one datagram per frame: a big-endian u16 length and the bytes.
 */
public final class UsbTunnel {
    public static final int VERSION = 1;
    public static final int PORT = 47900;
    // Moonlight's largest datagram is a 1408-byte video packet and ENet's MTU
    // tops out at 4096; anything longer means the stream is out of step
    public static final int MAX_PAYLOAD = 4096;

    public static final int HELLO_SIZE = 16;
    public static final int WELCOME_SIZE = 8;

    public static final int CHANNEL_PROBE = 0;
    public static final int CHANNEL_VIDEO = 1;
    public static final int CHANNEL_AUDIO = 2;
    public static final int CHANNEL_CONTROL = 3;

    public static final int STATUS_OK = 0;
    public static final int STATUS_VERSION_MISMATCH = 1;
    // The helper runs but Sunshine does not answer on the PC
    public static final int STATUS_HOST_UNREACHABLE = 2;
    public static final int STATUS_BAD_CHANNEL = 3;
    // Not on the wire: nothing answered on the tunnel port, so the helper is
    // not running or adb is not passing the port through
    public static final int STATUS_NO_HELPER = -1;

    private static final byte[] MAGIC = {'M', 'L', 'U', 'T'};

    private UsbTunnel() {}

    /** Whether a host address means the PC on the other end of the USB cable. */
    public static boolean isUsbHost(String address) {
        return address != null && address.startsWith("127.");
    }

    public static byte[] hello(int channel, long session) {
        ByteBuffer b = ByteBuffer.allocate(HELLO_SIZE);
        b.put(MAGIC).put((byte) VERSION).put((byte) channel).putShort((short) 0).putLong(session);
        return b.array();
    }

    /** Reads the helper's Welcome and returns its status. */
    public static int readWelcome(InputStream in) throws IOException {
        byte[] b = new byte[WELCOME_SIZE];
        new DataInputStream(in).readFully(b);
        for (int i = 0; i < MAGIC.length; i++) {
            if (b[i] != MAGIC[i]) {
                throw new IOException("usb tunnel: bad magic");
            }
        }
        return b[5] & 0xFF;
    }

    /**
     * Writes p[off, off+len) as one frame into dst at pos and returns the
     * position after it.
     */
    public static int putFrame(byte[] dst, int pos, byte[] p, int off, int len) {
        if (len > MAX_PAYLOAD) {
            throw new IllegalArgumentException("usb tunnel: payload " + len + " bytes exceeds " + MAX_PAYLOAD);
        }
        dst[pos] = (byte) (len >>> 8);
        dst[pos + 1] = (byte) len;
        System.arraycopy(p, off, dst, pos + 2, len);
        return pos + 2 + len;
    }

    /**
     * Reads one frame into buf, which must hold MAX_PAYLOAD bytes, and
     * returns its length. Throws EOFException at a clean end before a frame,
     * and IOException for a cut-off or oversized frame.
     */
    public static int readFrame(DataInputStream in, byte[] buf) throws IOException {
        int hi = in.read();
        if (hi < 0) {
            throw new EOFException();
        }
        int lo = in.read();
        if (lo < 0) {
            throw new IOException("usb tunnel: frame cut off");
        }
        int len = (hi << 8) | lo;
        if (len > MAX_PAYLOAD) {
            throw new IOException("usb tunnel: frame of " + len + " bytes exceeds " + MAX_PAYLOAD);
        }
        try {
            in.readFully(buf, 0, len);
        } catch (EOFException e) {
            throw new IOException("usb tunnel: frame cut off");
        }
        return len;
    }

    /**
     * Asks the helper whether it and Sunshine are there: STATUS_OK,
     * STATUS_HOST_UNREACHABLE, STATUS_VERSION_MISMATCH, or STATUS_NO_HELPER
     * when nothing answers. Blocks for up to about a second; not for the
     * UI thread.
     */
    public static int probe() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", PORT), 500);
            s.setSoTimeout(1500);
            s.getOutputStream().write(hello(CHANNEL_PROBE, 0));
            return readWelcome(s.getInputStream());
        } catch (IOException e) {
            // adb reverse accepts on the headset and drops the connection when
            // the PC side does not listen, so a missing helper shows up here
            // as a closed stream as often as a refused connect
            return STATUS_NO_HELPER;
        }
    }
}
