#!/bin/sh
# Runs the app's USB relay against the real usb-bridge helper, with a fake
# Sunshine behind it that echoes UDP and answers on its HTTP port, and adb
# replaced by a stub listing no devices. Needs go, python3 and the Android
# build environment; run from anywhere.
#
# SPDX-License-Identifier: GPL-3.0-or-later
set -eu
here=$(cd "$(dirname "$0")" && pwd)
repo=$(dirname "$here")
tmp=$(mktemp -d)
pids=""
cleanup() { for p in $pids; do kill "$p" 2>/dev/null || true; done; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

base=50000 # fake Sunshine's base port, clear of a real one on this machine

(cd "$here" && go build -o "$tmp/usb-bridge" .)
printf '#!/bin/sh\necho "List of devices attached"\n' > "$tmp/adb"
chmod +x "$tmp/adb"

cat > "$tmp/fakehost.py" <<'EOF'
import socket, sys, threading
base = int(sys.argv[1])
def echo(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
    s.bind(("127.0.0.1", port))
    while True:
        data, addr = s.recvfrom(65536)
        s.sendto(data, addr)
for off in (9, 10, 11):
    threading.Thread(target=echo, args=(base + off,), daemon=True).start()
http = socket.socket()
http.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
http.bind(("127.0.0.1", base))
http.listen()
while True:
    http.accept()[0].close()
EOF
python3 -I "$tmp/fakehost.py" $base & pids="$pids $!"
"$tmp/usb-bridge" -adb "$tmp/adb" -port $base & pids="$pids $!"
sleep 1

cd "$repo"
USB_BRIDGE_INTEROP=1 ./gradlew ${MOONLIGHT_GRADLE_ARGS:-} testNonRootDebugUnitTest \
    --tests com.limelight.nvstream.usb.UsbBridgeInteropTest -i 2>&1 \
    | grep -E "USB relay|UsbBridgeInteropTest >|BUILD|FAILED"
