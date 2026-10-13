// SPDX-License-Identifier: GPL-3.0-or-later
package main

import (
	"os/exec"
	"syscall"
)

// hideWindow keeps adb from flashing a console window when the helper runs
// in the background.
func hideWindow(c *exec.Cmd) {
	c.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
