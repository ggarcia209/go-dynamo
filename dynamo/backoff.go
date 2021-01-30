// Package dynamo contains controls and objects for DynamoDB CRUD operations.
// Operations in this package are abstracted from all other application logic
// and are designed to be used with any DynamoDB table and any object schema.
// This file contains objects for implementing an exponential backoff
// algorithm for DynamoDB error handling.
package dynamo

import (
	"math"
	"math/rand"
	"time"
)

// FailConfig stores parameters for the exponential backoff algorithm.
// Attempt, Elapsed, MaxRetiresReached should always be initialized to 0, 0, false.
type FailConfig struct {
	Base              float64
	Cap               float64
	Attempt           float64
	Elapsed           float64
	MaxRetriesReached bool
}

// DefaultFailConfig is the default configuration for the exponential backoff alogrithm
// with a base wait time of 50 miliseconds, and max wait time of 1 minute (60000 ms).
var DefaultFailConfig = &FailConfig{50, 60000, 0, 0, false}

// ExponentialBackoff implements the exponential backoff algorithm for request retries
// and returns true when the max number of retries has been reached (fc.Elapsed > fc.Cap).
func (fc *FailConfig) ExponentialBackoff() {
	if fc.Elapsed == fc.Cap {
		fc.MaxRetriesReached = true // max retries reached
		return
	}

	fc.Attempt += 1.0
	// exponential backoff with full jitter
	wait := float64(rand.Intn(int(fc.Base * math.Pow(2.0, fc.Attempt))))

	if fc.Elapsed+wait > fc.Cap {
		// wait until cap is reached
		time.Sleep(time.Duration(wait - (wait + fc.Elapsed - fc.Cap)))
		fc.Elapsed = fc.Cap
	}

	time.Sleep(time.Duration(wait) * time.Millisecond)
	fc.Elapsed += wait
}

// Reset resets Attempt and Elapsed fields.
func (fc *FailConfig) Reset() {
	fc.Attempt = 0
	fc.Elapsed = 0
	fc.MaxRetriesReached = false
}
