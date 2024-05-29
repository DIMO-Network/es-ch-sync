package sync

import (
	"time"
)

type Options struct {
	// StartTime is the time is the earliest time to start syncing data.
	StartTime time.Time

	// StopTime is the time latest time to stop syncing data. (default: time.Now())
	StopTime time.Time

	// BatchSize is the number of records to process at a time. (default: 1000)
	BatchSize int

	// TokenIDs is the list of token IDs to sync. If empty, all token IDs are synced.
	TokenIDs []string

	// Signals is the list of signals to sync. If empty, all signals are synced.
	Signals []string
}
