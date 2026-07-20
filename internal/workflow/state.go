package workflow

import (
	"math"
	"time"
)

var transitions = map[string]map[string]bool{
	"BLOCKED":     {"READY": true, "CANCELLED": true},
	"READY":       {"DISPATCHING": true, "CANCELLED": true},
	"DISPATCHING": {"LEASED": true, "READY": true, "CANCELLED": true},
	"LEASED":      {"RUNNING": true, "RETRY_WAIT": true, "DEAD": true, "CANCELLED": true},
	"RUNNING":     {"SUCCEEDED": true, "RETRY_WAIT": true, "DEAD": true, "CANCELLED": true},
	"RETRY_WAIT":  {"READY": true, "CANCELLED": true},
}

func CanTransition(from, to string) bool { return transitions[from][to] }

func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	seconds := math.Min(3600, 2*math.Pow(2, float64(attempt-1)))
	return time.Duration(seconds * float64(time.Second))
}
