// Package retry provides a small, zero-dependency helper for retrying
// fallible operations with exponential backoff, while respecting
// context cancellation.
package retry

import (
	"context"
	"time"
)

// Option customizes the behavior of Do.
type Option func(*config)

// config holds the tunable behavior applied by Options.
type config struct {
	retryIf func(error) bool
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

		if err := sleep(ctx, delay); err != nil {
			return err
		}

		delay = time.Duration(float64(delay) * backoffMultiplier)
	}

	return lastErr
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
