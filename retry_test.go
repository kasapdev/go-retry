package retry

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// errSentinel is used to identify a particular failure in tests.
var errSentinel = errors.New("sentinel failure")

func TestDo_SucceedsAfterNFailures(t *testing.T) {
	tests := []struct {
		name       string
		failCount  int
		maxAttempt int
	}{
		{"succeeds on first attempt", 0, 5},
		{"succeeds after 1 failure", 1, 5},
		{"succeeds after 3 failures", 3, 5},
		{"succeeds on last possible attempt", 4, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int32

			fn := func() error {
				n := atomic.AddInt32(&calls, 1)
				if int(n) <= tt.failCount {
					return errSentinel
				}
				return nil
			}

			err := Do(context.Background(), tt.maxAttempt, time.Millisecond, 2, fn)
			if err != nil {
				t.Fatalf("Do() returned error %v, want nil", err)
			}

			wantCalls := int32(tt.failCount + 1)
			if calls != wantCalls {
				t.Fatalf("fn called %d times, want exactly %d", calls, wantCalls)
			}
		})
	}
}

func TestDo_ExhaustsAttemptsOnPersistentFailure(t *testing.T) {
	var calls int32
	const maxAttempts = 3

	fn := func() error {
		atomic.AddInt32(&calls, 1)
		return errSentinel
	}

	err := Do(context.Background(), maxAttempts, time.Millisecond, 2, fn)
	if err == nil {
		t.Fatal("Do() returned nil error, want non-nil")
	}
	if !errors.Is(err, errSentinel) {
		t.Fatalf("Do() returned error %v, want it to wrap/equal errSentinel", err)
	}
	if calls != maxAttempts {
		t.Fatalf("fn called %d times, want exactly %d", calls, maxAttempts)
	}
}

func TestDo_RetryIfStopsOnNonRetryableError(t *testing.T) {
	var calls int32
	errFatal := errors.New("fatal, do not retry")

	fn := func() error {
		atomic.AddInt32(&calls, 1)
		return errFatal
	}

	notRetryable := func(err error) bool {
		return !errors.Is(err, errFatal)
	}

	err := Do(context.Background(), 10, time.Millisecond, 2, fn, RetryIf(notRetryable))
	if !errors.Is(err, errFatal) {
		t.Fatalf("Do() returned error %v, want errFatal", err)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times, want exactly 1 (should stop immediately on non-retryable error)", calls)
	}
}

func TestDo_RetryIfAllowsSelectiveRetry(t *testing.T) {
	// Only errSentinel is retryable; a different error should stop immediately.
	errOther := errors.New("other error")
	var calls int32

	fn := func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return errSentinel
		}
		return errOther
	}

	retryOnlySentinel := func(err error) bool {
		return errors.Is(err, errSentinel)
	}

	err := Do(context.Background(), 10, time.Millisecond, 2, fn, RetryIf(retryOnlySentinel))
	if !errors.Is(err, errOther) {
		t.Fatalf("Do() returned error %v, want errOther", err)
	}
	if calls != 3 {
		t.Fatalf("fn called %d times, want exactly 3", calls)
	}
}

func TestDo_ContextCancellationDuringBackoffSleep(t *testing.T) {
	// initialDelay is long relative to the cancellation trigger, but the
	// overall test must still complete fast: we assert Do returns well
	// before the full retry schedule would have elapsed.
	const initialDelay = 200 * time.Millisecond
	const cancelAfter = 50 * time.Millisecond

	var calls int32
	fn := func() error {
		atomic.AddInt32(&calls, 1)
		return errSentinel
	}

	ctx, cancel := context.WithTimeout(context.Background(), cancelAfter)
	defer cancel()

	start := time.Now()
	// maxAttempts is high enough that, without cancellation, this would
	// take initialDelay + initialDelay*2 + initialDelay*4 + ... which
	// vastly exceeds cancelAfter.
	err := Do(ctx, 10, initialDelay, 2, fn)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() returned error %v, want errors.Is(err, context.DeadlineExceeded)", err)
	}

	// The full schedule (10 attempts starting at 200ms, doubling) would take
	// seconds. We should abort not long after cancelAfter. Give a generous
	// margin for slow/race-instrumented CI, but well under what the full
	// schedule would need.
	const maxAllowed = 2 * time.Second
	if elapsed >= maxAllowed {
		t.Fatalf("Do() took %v, want well under %v (should abort promptly on ctx cancellation)", elapsed, maxAllowed)
	}

	// Should have had time for at least the first attempt.
	if calls < 1 {
		t.Fatalf("fn called %d times, want at least 1", calls)
	}
}

func TestDo_ContextAlreadyCanceledBeforeStart(t *testing.T) {
	var calls int32
	fn := func() error {
		atomic.AddInt32(&calls, 1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Do(ctx, 5, time.Millisecond, 2, fn)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() returned error %v, want errors.Is(err, context.Canceled)", err)
	}
	if calls != 0 {
		t.Fatalf("fn called %d times, want 0 (context was already canceled)", calls)
	}
}

func TestMaxDelay_CapsTheBackoff(t *testing.T) {
	var delays []time.Duration
	err := Do(context.Background(), 5, time.Millisecond, 2.0,
		func() error { return errSentinel },
		MaxDelay(3*time.Millisecond),
		OnRetry(func(_ int, _ error, d time.Duration) { delays = append(delays, d) }),
	)
	if !errors.Is(err, errSentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 3 * time.Millisecond}
	if len(delays) != len(want) {
		t.Fatalf("got %d delays %v, want %d %v", len(delays), delays, len(want), want)
	}
	for i := range want {
		if delays[i] != want[i] {
			t.Errorf("delay[%d] = %v, want %v", i, delays[i], want[i])
		}
	}
}

func TestMaxDelay_ZeroMeansNoCap(t *testing.T) {
	var delays []time.Duration
	_ = Do(context.Background(), 4, time.Millisecond, 3.0,
		func() error { return errSentinel },
		MaxDelay(0),
		OnRetry(func(_ int, _ error, d time.Duration) { delays = append(delays, d) }),
	)
	want := []time.Duration{time.Millisecond, 3 * time.Millisecond, 9 * time.Millisecond}
	for i := range want {
		if delays[i] != want[i] {
			t.Errorf("delay[%d] = %v, want %v", i, delays[i], want[i])
		}
	}
}

func TestNextDelay_NeverOverflowsToNegative(t *testing.T) {
	got := nextDelay(time.Duration(1<<62), 10.0, 0)
	if got <= 0 {
		t.Fatalf("nextDelay overflowed to %v", got)
	}
	if got := nextDelay(time.Hour, 10.0, time.Minute); got != time.Minute {
		t.Fatalf("nextDelay with cap = %v, want 1m", got)
	}
}

func TestOnRetry_ReportsAttemptAndError(t *testing.T) {
	type call struct {
		attempt int
		err     error
	}
	var calls []call
	n := 0
	err := Do(context.Background(), 4, time.Millisecond, 1.0,
		func() error {
			n++
			if n < 3 {
				return errSentinel
			}
			return nil
		},
		OnRetry(func(attempt int, err error, _ time.Duration) { calls = append(calls, call{attempt, err}) }),
	)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(calls) != 2 || calls[0].attempt != 1 || calls[1].attempt != 2 {
		t.Fatalf("unexpected OnRetry calls: %+v", calls)
	}
	if !errors.Is(calls[0].err, errSentinel) {
		t.Errorf("OnRetry got err %v, want sentinel", calls[0].err)
	}
}

func TestOnRetry_NotCalledAfterFinalAttemptOrNonRetryableError(t *testing.T) {
	calls := 0
	hook := OnRetry(func(int, error, time.Duration) { calls++ })

	// Final attempt fails: only attempts 1 and 2 are followed by a retry.
	_ = Do(context.Background(), 3, time.Millisecond, 1.0, func() error { return errSentinel }, hook)
	if calls != 2 {
		t.Fatalf("OnRetry called %d times for 3 failing attempts, want 2", calls)
	}

	// Non-retryable error: no retry follows, so no callback.
	calls = 0
	_ = Do(context.Background(), 3, time.Millisecond, 1.0, func() error { return errSentinel }, hook,
		RetryIf(func(error) bool { return false }))
	if calls != 0 {
		t.Fatalf("OnRetry called %d times for a non-retryable error, want 0", calls)
	}
}
