// Package retry provides a small, zero-dependency helper for retrying
// fallible operations with exponential backoff, while respecting
// context cancellation.
package retry

import (
	"context"
	"math"
	"time"
)

// Option customizes the behavior of Do.
type Option func(*config)

// config holds the tunable behavior applied by Options.
type config struct {
	retryIf  func(error) bool
	maxDelay time.Duration
	onRetry  func(attempt int, err error, delay time.Duration)
}

// MaxDelay returns an Option that caps the backoff delay. Once the
// exponentially growing delay would exceed d, every later wait is exactly d.
// A d of zero or less means "no cap" (the default).
//
// Besides keeping worst-case latency predictable, a cap prevents the delay
// from overflowing time.Duration when backoffMultiplier is large or
// maxAttempts is high.
func MaxDelay(d time.Duration) Option {
	return func(c *config) {
		c.maxDelay = d
	}
}

// OnRetry returns an Option that registers a callback invoked right before
// Do sleeps ahead of a retry. It receives the 1-based number of the attempt
// that just failed, that attempt's error, and the delay Do is about to wait
// (after any MaxDelay cap). It is not called after the final attempt, nor
// when RetryIf reports the error as non-retryable, since no retry follows in
// either case. Use it for logging or metrics.
func OnRetry(fn func(attempt int, err error, delay time.Duration)) Option {
	return func(c *config) {
		c.onRetry = fn
	}
}

// RetryIf returns an Option that overrides which errors are considered
// retryable. The predicate is called with the error returned by fn after
// each failed attempt; if it returns false, Do stops immediately and
// returns that error without making further attempts.
//
// If RetryIf is not supplied, Do treats every non-nil error as retryable.
func RetryIf(predicate func(err error) bool) Option {
	return func(c *config) {
		c.retryIf = predicate
	}
}

// Do calls fn, retrying on failure with exponential backoff.
//
// fn is called at most maxAttempts times. Before each retry (that is,
// after every failed attempt except the last), Do waits initialDelay,
// then initialDelay*backoffMultiplier, then
// initialDelay*backoffMultiplier^2, and so on, doubling (or scaling by
// backoffMultiplier) after every subsequent failure.
//
// Do returns nil as soon as fn succeeds. If fn fails on every attempt,
// Do returns the error from the last attempt. By default any non-nil
// error is considered retryable; pass RetryIf to customize this — when
// the predicate reports an error as non-retryable, Do returns that
// error immediately without exhausting maxAttempts.
//
// Do honors ctx: if ctx is done (canceled or its deadline is exceeded),
// Do stops as soon as possible and returns ctx.Err(), even if that
// happens in the middle of a backoff sleep. Callers that need the
// underlying fn error alongside cancellation should inspect ctx.Err()
// themselves; Do returns ctx.Err() so callers can use errors.Is with
// context.Canceled or context.DeadlineExceeded.
//
// maxAttempts must be at least 1. initialDelay is the delay before the
// second attempt; it is not applied before the first call to fn.
func Do(ctx context.Context, maxAttempts int, initialDelay time.Duration, backoffMultiplier float64, fn func() error, opts ...Option) error {
	cfg := config{
		retryIf: func(error) bool { return true },
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if maxAttempts < 1 {
		maxAttempts = 1
	}

	delay := initialDelay
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Stop before making an attempt if the context is already done.
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if !cfg.retryIf(lastErr) {
			return lastErr
		}

		// No need to sleep after the last attempt.
		if attempt == maxAttempts {
			break
		}

		wait := capDelay(delay, cfg.maxDelay)
		if cfg.onRetry != nil {
			cfg.onRetry(attempt, lastErr, wait)
		}

		if err := sleep(ctx, wait); err != nil {
			return err
		}

		delay = nextDelay(delay, backoffMultiplier, cfg.maxDelay)
	}

	return lastErr
}

// capDelay returns d limited to max when max is positive.
func capDelay(d, max time.Duration) time.Duration {
	if max > 0 && d > max {
		return max
	}
	return d
}

// nextDelay scales d by multiplier, clamping to max (when positive) and to
// the largest representable Duration so the result can never overflow into a
// negative value, which would make the next sleep return immediately.
func nextDelay(d time.Duration, multiplier float64, max time.Duration) time.Duration {
	next := float64(d) * multiplier
	if max > 0 && next > float64(max) {
		return max
	}
	if next >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(next)
}

// sleep blocks for d, or returns ctx.Err() promptly if ctx is done first.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
