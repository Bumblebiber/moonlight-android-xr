//go:build !windows

// SPDX-License-Identifier: GPL-3.0-or-later
package main

import "os/exec"

func hideWindow(*exec.Cmd) {}
