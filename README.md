# go-retry

A small, zero-dependency Go library for retrying fallible operations with
exponential backoff. It respects `context.Context` cancellation at every
step, including mid-sleep during backoff, and lets you customize which
errors are worth retrying.

## Installation

```
go get github.com/kasapdev/go-retry
```

## Usage

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/kasapdev/go-retry"
)

// simulate a flaky operation that fails twice, then succeeds
func flakyOperation() func() error {
	attempts := 0
	return func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("attempt %d: transient failure", attempts)
		}
		return nil
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	op := flakyOperation()

	err := retry.Do(ctx, 5, 100*time.Millisecond, 2.0, op)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			log.Fatalf("operation aborted: context done: %v", err)
		}
		log.Fatalf("operation failed after retries: %v", err)
	}

	fmt.Println("operation succeeded")
}
```

### Customizing which errors are retryable

By default, `Do` retries on any non-nil error. Use `RetryIf` to stop
retrying immediately for errors that will never succeed on retry (for
example, a 4xx HTTP client error):

```go
var errNotFound = errors.New("resource not found")

isRetryable := func(err error) bool {
	// don't waste attempts on a 404-style error
	return !errors.Is(err, errNotFound)
}

err := retry.Do(ctx, 5, 100*time.Millisecond, 2.0, op, retry.RetryIf(isRetryable))
```

### Capping the delay and observing retries

`MaxDelay` stops the exponential backoff from growing past a ceiling, and
`OnRetry` is called right before each wait, which is handy for logging or
metrics:

```go
err := retry.Do(ctx, 8, 100*time.Millisecond, 2.0, op,
	retry.MaxDelay(2*time.Second), // waits: 100ms, 200ms, 400ms, 800ms, 1.6s, 2s, 2s
	retry.OnRetry(func(attempt int, err error, delay time.Duration) {
		log.Printf("attempt %d failed (%v); retrying in %v", attempt, err, delay)
	}),
)
```

## API

### `func Do(ctx context.Context, maxAttempts int, initialDelay time.Duration, backoffMultiplier float64, fn func() error, opts ...Option) error`

Calls `fn`, retrying on failure with exponential backoff.

- `fn` is called at most `maxAttempts` times.
- Before each retry, `Do` waits `initialDelay`, then
  `initialDelay * backoffMultiplier`, then
  `initialDelay * backoffMultiplier^2`, and so on.
- Returns `nil` as soon as `fn` succeeds.
- If `fn` fails on every attempt, returns the error from the last attempt.
- If `ctx` is canceled or its deadline is exceeded — including in the
  middle of a backoff sleep — `Do` returns promptly with `ctx.Err()`.

### `type Option`

`Option` customizes `Do`'s behavior. The available options are:

### `func RetryIf(predicate func(err error) bool) Option`

Overrides which errors are considered retryable. `predicate` is called
with the error from each failed attempt; if it returns `false`, `Do`
stops immediately and returns that error without making further
attempts. If `RetryIf` is not supplied, every non-nil error is retried.

### `func MaxDelay(d time.Duration) Option`

Caps the backoff delay: once the growing delay would exceed `d`, every later
wait is exactly `d`. `d <= 0` means no cap (the default). The cap also
prevents the delay from overflowing `time.Duration` when the multiplier or
attempt count is large.

### `func OnRetry(fn func(attempt int, err error, delay time.Duration)) Option`

Registers a callback invoked right before `Do` sleeps ahead of a retry, with
the 1-based number of the attempt that just failed, its error, and the delay
about to be waited (after any `MaxDelay` cap). It is not called after the
final attempt or when `RetryIf` reports the error as non-retryable, since no
retry follows in those cases.

## Testing

```
go test ./...
```

Run with the race detector and verbose output (as CI does):

```
go test ./... -race -v
```

## License

MIT — see [LICENSE](LICENSE).
