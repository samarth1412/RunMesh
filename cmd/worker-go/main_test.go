package main

import "testing"

func TestSupportedHandlers(t *testing.T) {
	for _, handler := range []string{"examples.greet", "examples.upper", "examples.notify", "examples.slow", "validation.dead"} {
		if !supported(handler) {
			t.Errorf("supported(%q)=false", handler)
		}
	}
	if supported("documents.unknown") {
		t.Fatal("unknown handler must not be claimed by this worker pool")
	}
}
