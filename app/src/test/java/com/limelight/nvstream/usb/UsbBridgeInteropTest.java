package com.limelight.nvstream.usb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.DatagramSocket;
import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * This relay against the real usb-bridge helper, with a fake Sunshine that
 * echoes UDP behind it. Runs only from usb-bridge/interop-test.sh, which
 * starts the helper and the echo and sets USB_BRIDGE_INTEROP.
 */
public class UsbBridgeInteropTest {

    private UsbRelay relay;
    private DatagramSocket moonlight;

    @Before
    public void setUp() throws Exception {
        assumeTrue("needs usb-bridge/interop-test.sh", System.getenv("USB_BRIDGE_INTEROP") != null);
        relay = new UsbRelay(UsbTunnel.PORT, 0, 0, 0);
        relay.start();
        moonlight = new DatagramSocket(0, InetAddress.getByName("127.0.0.1"));
    }

    @After
    public void tearDown() {
        if (relay != null) {
            relay.stop();
            moonlight.close();
        }
    }

    @Test
    public void probeSeesHelperAndHost() {
        assertEquals(UsbTunnel.STATUS_OK, UsbTunnel.probe());
    }

    @Test
    public void streamsThroughTheHelper() throws Exception {
        for (int ch = UsbTunnel.CHANNEL_VIDEO; ch <= UsbTunnel.CHANNEL_CONTROL; ch++) {
            long deadline = System.currentTimeMillis() + 5000;
            while (relay.getStatus(ch) != UsbTunnel.STATUS_OK) {
                assertTrue("channel " + ch + " not up", System.currentTimeMillis() < deadline);
                Thread.sleep(50);
            }
        }
        double loss = UsbRelayTest.streamRate(moonlight, relay.getLocalPort(UsbTunnel.CHANNEL_VIDEO),
                "USB relay + usb-bridge + fake Sunshine");
        assertTrue("loss " + loss, loss < 0.02);
    }
}
