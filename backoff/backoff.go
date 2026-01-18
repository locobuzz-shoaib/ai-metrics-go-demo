// Package backoff provides exponential backoff functionality for retry operations.
package backoff

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Config defines exponential backoff parameters
type Config struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxRetries      int     // 0 = unlimited
	Jitter          float64 // 0.0 to 1.0 - adds randomness to prevent thundering herd
}

// DefaultConfig returns sensible defaults for Kafka consumer
func DefaultConfig() Config {
	return Config{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
		MaxRetries:      0, // Unlimited for consumer
		Jitter:          0.1,
	}
}

// RetryConfig returns config suitable for retry operations (limited retries)
func RetryConfig() Config {
	return Config{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		MaxRetries:      3,
		Jitter:          0.1,
	}
}

// Backoff tracks retry state and calculates delays
type Backoff struct {
	cfg      Config
	attempts int
	rng      *rand.Rand
}

// New creates a new Backoff instance
func New(cfg Config) *Backoff {
	return &Backoff{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Next returns the next backoff duration and increments attempt counter
// Returns false if max retries exceeded
func (b *Backoff) Next() (time.Duration, bool) {
	if b.cfg.MaxRetries > 0 && b.attempts >= b.cfg.MaxRetries {
		return 0, false
	}

	duration := float64(b.cfg.InitialInterval) * math.Pow(b.cfg.Multiplier, float64(b.attempts))

	if duration > float64(b.cfg.MaxInterval) {
		duration = float64(b.cfg.MaxInterval)
	}

	// Add jitter to prevent thundering herd
	if b.cfg.Jitter > 0 {
		jitter := duration * b.cfg.Jitter * (2*b.rng.Float64() - 1)
		duration += jitter
	}

	b.attempts++
	return time.Duration(duration), true
}

// Reset resets the attempt counter (call on success)
func (b *Backoff) Reset() {
	b.attempts = 0
}

// Attempts returns the current attempt count
func (b *Backoff) Attempts() int {
	return b.attempts
}

// Wait waits for the backoff duration, respecting context cancellation
// Returns false if max retries exceeded or context cancelled
func (b *Backoff) Wait(ctx context.Context) bool {
	delay, ok := b.Next()
	if !ok {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}

// Duration returns the current backoff duration without incrementing
func (b *Backoff) Duration() time.Duration {
	duration := float64(b.cfg.InitialInterval) * math.Pow(b.cfg.Multiplier, float64(b.attempts))
	if duration > float64(b.cfg.MaxInterval) {
		duration = float64(b.cfg.MaxInterval)
	}
	return time.Duration(duration)
}
