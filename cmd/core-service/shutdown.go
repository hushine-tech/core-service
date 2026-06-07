package main

import "time"

func waitForBackgroundTasks(timeout time.Duration, wait func(), onTimeout func()) bool {
	if wait == nil {
		return true
	}
	if timeout <= 0 {
		wait()
		return true
	}
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		if onTimeout != nil {
			onTimeout()
		}
		return false
	}
}
