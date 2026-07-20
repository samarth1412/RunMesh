package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestSplitNonEmpty(t *testing.T) {
	got := splitNonEmpty(" https://one.example,https://two.example, ,")
	want := []string{"https://one.example", "https://two.example"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitNonEmpty()=%v, want %v", got, want)
	}
}

func TestOutboxClaimConfiguration(t *testing.T) {
	t.Setenv("RUNMESH_DEV_AUTH", "true")
	t.Setenv("RUNMESH_OUTBOX_CLAIM_TTL", "5s")
	t.Setenv("RUNMESH_OUTBOX_PUBLISH_TIMEOUT", "5s")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "publish timeout") {
		t.Fatalf("Load() error=%v, want outbox timeout validation", err)
	}
}
