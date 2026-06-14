package main

import (
	"context"
	"testing"
	"time"
)

type orderUserDataStreamManagerStub struct {
	calls  int
	cancel context.CancelFunc
}

func (s *orderUserDataStreamManagerStub) SyncOnce(context.Context) (int, error) {
	s.calls++
	if s.cancel != nil {
		s.cancel()
	}
	return 0, nil
}

func TestRunOrderUserDataStreamManagerRunsInitialSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stub := &orderUserDataStreamManagerStub{cancel: cancel}

	done := make(chan struct{})
	go func() {
		runOrderUserDataStreamManager(ctx, stub, time.Hour)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner")
	}
	if stub.calls != 1 {
		t.Fatalf("SyncOnce calls = %d, want 1", stub.calls)
	}
}
