package config

import (
	"reflect"
	"testing"
)

func TestSplitNonEmpty(t *testing.T) {
	got := splitNonEmpty(" https://one.example,https://two.example, ,")
	want := []string{"https://one.example", "https://two.example"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitNonEmpty()=%v, want %v", got, want)
	}
}
