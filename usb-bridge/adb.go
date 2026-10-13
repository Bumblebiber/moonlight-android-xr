// SPDX-License-Identifier: GPL-3.0-or-later
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// findAdb returns the adb to use: the flag, then $ADB, then PATH, then the
// usual SDK locations.
func findAdb(flagPath string) (string, error) {
	if flagPath != "" {
		return flagPath, nil
	}
	if p := os.Getenv("ADB"); p != "" {
		return p, nil
	}
	if p, err := exec.LookPath("adb"); err == nil {
		return p, nil
	}
	exe := "adb"
	if runtime.GOOS == "windows" {
		exe = "adb.exe"
	}
	var dirs []string
	for _, env := range []string{"ANDROID_HOME", "ANDROID_SDK_ROOT"} {
		if v := os.Getenv(env); v != "" {
			dirs = append(dirs, filepath.Join(v, "platform-tools"))
		}
	}
	home, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "windows":
		dirs = append(dirs, filepath.Join(os.Getenv("LOCALAPPDATA"), "Android", "platform-tools"),
			filepath.Join(os.Getenv("LOCALAPPDATA"), "Android", "Sdk", "platform-tools"))
	case "darwin":
		dirs = append(dirs, filepath.Join(home, "Library", "Android", "sdk", "platform-tools"))
	default:
		dirs = append(dirs, filepath.Join(home, "Android", "Sdk", "platform-tools"))
	}
	for _, d := range dirs {
		p := filepath.Join(d, exe)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("adb not found: install Android platform-tools or pass -adb")
}

// reverseRule is one adb reverse mapping, headset port to PC port.
type reverseRule struct{ device, local int }

func (r reverseRule) args() (string, string) {
	return fmt.Sprintf("tcp:%d", r.device), fmt.Sprintf("tcp:%d", r.local)
}

// adbWatcher keeps the reverse rules on every attached, authorized headset.
// Rules die with the USB connection, so it checks once a second and puts
// them back after a replug.
type adbWatcher struct {
	adb    string
	serial string // only this device when set
	rules  []reverseRule

	state map[string]string // last logged state per serial
}

func (w *adbWatcher) run(ctx context.Context) {
	w.state = map[string]string{}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		w.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *adbWatcher) poll(ctx context.Context) {
	out, err := w.cmd(ctx, "", "devices")
	if err != nil {
		w.note("", "adb error: "+err.Error())
		return
	}
	seen := map[string]bool{}
	for _, d := range parseDevices(out) {
		if w.serial != "" && d.serial != w.serial {
			continue
		}
		seen[d.serial] = true
		switch d.state {
		case "device":
			if err := w.ensureRules(ctx, d.serial); err != nil {
				w.note(d.serial, "reverse failed: "+err.Error())
			} else {
				w.note(d.serial, "ready")
			}
		case "unauthorized":
			w.note(d.serial, "unauthorized: confirm \"Allow USB debugging\" in the headset")
		default:
			w.note(d.serial, d.state)
		}
	}
	for s := range w.state {
		if s != "" && !seen[s] {
			w.note(s, "gone")
			delete(w.state, s)
		}
	}
}

func (w *adbWatcher) ensureRules(ctx context.Context, serial string) error {
	list, err := w.cmd(ctx, serial, "reverse", "--list")
	if err != nil {
		return err
	}
	for _, r := range w.rules {
		dev, local := r.args()
		if strings.Contains(list, " "+dev+" "+local) {
			continue
		}
		if _, err := w.cmd(ctx, serial, "reverse", dev, local); err != nil {
			return err
		}
	}
	return nil
}

func (w *adbWatcher) cmd(ctx context.Context, serial string, args ...string) (string, error) {
	if serial != "" {
		args = append([]string{"-s", serial}, args...)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, w.adb, args...)
	hideWindow(c)
	out, err := c.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// note logs a device's state when it changes.
func (w *adbWatcher) note(serial, state string) {
	if w.state[serial] == state {
		return
	}
	w.state[serial] = state
	if serial == "" {
		log.Print(state)
	} else {
		log.Printf("device %s: %s", serial, state)
	}
}

type device struct{ serial, state string }

// parseDevices reads `adb devices` output.
func parseDevices(out string) []device {
	var ds []device
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] == "List" || strings.HasPrefix(f[0], "*") {
			continue
		}
		ds = append(ds, device{f[0], f[1]})
	}
	return ds
}
