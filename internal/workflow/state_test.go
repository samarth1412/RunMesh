package workflow

import "testing"

func TestStateTransitions(t *testing.T) {
	allowed := [][2]string{{"BLOCKED", "READY"}, {"READY", "DISPATCHING"}, {"DISPATCHING", "LEASED"}, {"LEASED", "RUNNING"}, {"RUNNING", "SUCCEEDED"}, {"RUNNING", "RETRY_WAIT"}, {"RETRY_WAIT", "READY"}}
	for _, pair := range allowed {
		if !CanTransition(pair[0], pair[1]) {
			t.Errorf("expected %s -> %s", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{{"BLOCKED", "SUCCEEDED"}, {"READY", "RUNNING"}, {"SUCCEEDED", "READY"}, {"DEAD", "RUNNING"}} {
		if CanTransition(pair[0], pair[1]) {
			t.Errorf("unexpected %s -> %s", pair[0], pair[1])
		}
	}
}
