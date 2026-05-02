package kv

import (
	"errors"
	"testing"
	"time"

	"peacock/wal"
)

type fakeCompactable struct {
	calls   chan int
	err     error
	trigger chan int
}

func newFakeCompactable() *fakeCompactable {
	return &fakeCompactable{
		calls:   make(chan int, 16),
		trigger: make(chan int, 16),
	}
}

func (f *fakeCompactable) Compact(trigger int, _ func(*wal.Entry) ([]byte, error)) (bool, error) {
	f.trigger <- trigger
	f.calls <- 1
	if f.err != nil {
		return false, f.err
	}
	return false, nil
}

func TestCompactorTicksAndStops(t *testing.T) {
	target := newFakeCompactable()
	c := newCompactor(target, 4, 20*time.Millisecond, nil)
	c.start()
	t.Cleanup(c.stop)

	for i := 0; i < 2; i++ {
		select {
		case <-target.calls:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected tick %d within 200ms", i+1)
		}
	}

	select {
	case got := <-target.trigger:
		if got != 4 {
			t.Fatalf("expected trigger=4, got %d", got)
		}
	default:
	}
}

func TestCompactorReportsErrors(t *testing.T) {
	target := newFakeCompactable()
	target.err = errors.New("boom")
	captured := make(chan error, 1)
	onError := func(err error) {
		select {
		case captured <- err:
		default:
		}
	}

	c := newCompactor(target, 4, 20*time.Millisecond, onError)
	c.start()
	t.Cleanup(c.stop)

	select {
	case got := <-captured:
		if got == nil || got.Error() != "boom" {
			t.Fatalf("expected captured 'boom', got %v", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("onError was not called within 200ms")
	}
}

func TestCompactorStopReturnsPromptly(t *testing.T) {
	target := newFakeCompactable()
	c := newCompactor(target, 4, 1*time.Hour, nil)
	c.start()

	done := make(chan struct{})
	go func() {
		c.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("stop did not return promptly")
	}
}
