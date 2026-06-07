package main

import (
	"testing"
	"time"
)

func TestWaitForBackgroundTasksWaitsForCompletion(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan bool, 1)

	go func() {
		done <- waitForBackgroundTasks(200*time.Millisecond, func() {
			close(started)
			<-release
		}, nil)
	}()

	<-started
	select {
	case <-done:
		t.Fatal("waitForBackgroundTasks returned before background work completed")
	default:
	}

	close(release)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("waitForBackgroundTasks returned false after background work completed")
		}
	case <-time.After(time.Second):
		t.Fatal("waitForBackgroundTasks did not return after background work completed")
	}
}

func TestWaitForBackgroundTasksTimesOut(t *testing.T) {
	timedOut := false
	ok := waitForBackgroundTasks(10*time.Millisecond, func() {
		select {}
	}, func() {
		timedOut = true
	})
	if ok {
		t.Fatal("waitForBackgroundTasks returned true for blocked background work")
	}
	if !timedOut {
		t.Fatal("timeout hook was not called")
	}
}
