package workflow

import "testing"

func TestValidateDAG(t *testing.T) {
	tests := []struct {
		name    string
		dag     DAG
		wantErr bool
	}{
		{"valid", DAG{Tasks: map[string]TaskSpec{"a": {Handler: "x.a"}, "b": {Handler: "x.b", DependsOn: []string{"a"}}}}, false},
		{"cycle", DAG{Tasks: map[string]TaskSpec{"a": {Handler: "x.a", DependsOn: []string{"b"}}, "b": {Handler: "x.b", DependsOn: []string{"a"}}}}, true},
		{"missing", DAG{Tasks: map[string]TaskSpec{"a": {Handler: "x.a", DependsOn: []string{"nope"}}}}, true},
		{"self", DAG{Tasks: map[string]TaskSpec{"a": {Handler: "x.a", DependsOn: []string{"a"}}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateDAG(tt.dag); (got != nil) != tt.wantErr {
				t.Fatalf("ValidateDAG() error = %v", got)
			}
		})
	}
}

func TestRetryDelay(t *testing.T) {
	if got := RetryDelay(1).Seconds(); got != 2 {
		t.Fatalf("first delay = %v", got)
	}
	if got := RetryDelay(20).Seconds(); got != 3600 {
		t.Fatalf("delay cap = %v", got)
	}
}
