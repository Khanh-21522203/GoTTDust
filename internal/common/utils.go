package common

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// NowMicros returns the current time in Unix microseconds.
func NowMicros() int64 {
	return time.Now().UnixMicro()
}

// TimeFromMicros converts Unix microseconds to time.Time.
func TimeFromMicros(micros int64) time.Time {
	return time.UnixMicro(micros)
}

// GenerateTraceID generates a random 16-byte hex trace ID.
func GenerateTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// MinInt returns the smaller of two ints.
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MaxInt returns the larger of two ints.
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ClampInt clamps a value between min and max.
func ClampInt(val, min, max int) int {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}
