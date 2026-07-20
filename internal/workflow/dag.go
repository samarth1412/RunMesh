package workflow

import (
	"fmt"
	"regexp"
)

const MaxTasks = 500
const MaxEdges = 5000

var taskKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)

func ValidateDAG(dag DAG) error {
	if len(dag.Tasks) == 0 {
		return fmt.Errorf("workflow requires at least one task")
	}
	if len(dag.Tasks) > MaxTasks {
		return fmt.Errorf("workflow exceeds %d tasks", MaxTasks)
	}
	edges := 0
	for key, task := range dag.Tasks {
		if !taskKeyPattern.MatchString(key) {
			return fmt.Errorf("invalid task key %q", key)
		}
		if !taskKeyPattern.MatchString(task.Handler) {
			return fmt.Errorf("task %q has invalid handler", key)
		}
		if task.MaximumAttempts < 0 || task.MaximumAttempts > 100 {
			return fmt.Errorf("task %q maximum_attempts must be between 1 and 100", key)
		}
		if task.TimeoutSeconds < 0 || task.TimeoutSeconds > 86400 {
			return fmt.Errorf("task %q timeout_seconds must be between 1 and 86400", key)
		}
		seen := map[string]bool{}
		for _, dep := range task.DependsOn {
			edges++
			if dep == key {
				return fmt.Errorf("task %q cannot depend on itself", key)
			}
			if _, ok := dag.Tasks[dep]; !ok {
				return fmt.Errorf("task %q depends on missing task %q", key, dep)
			}
			if seen[dep] {
				return fmt.Errorf("task %q repeats dependency %q", key, dep)
			}
			seen[dep] = true
		}
	}
	if edges > MaxEdges {
		return fmt.Errorf("workflow exceeds %d dependency edges", MaxEdges)
	}
	color := make(map[string]uint8, len(dag.Tasks))
	var visit func(string) error
	visit = func(key string) error {
		if color[key] == 1 {
			return fmt.Errorf("workflow contains a cycle involving %q", key)
		}
		if color[key] == 2 {
			return nil
		}
		color[key] = 1
		for _, dep := range dag.Tasks[key].DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}
		color[key] = 2
		return nil
	}
	for key := range dag.Tasks {
		if err := visit(key); err != nil {
			return err
		}
	}
	return nil
}

func WithDefaults(spec TaskSpec) TaskSpec {
	if spec.MaximumAttempts == 0 {
		spec.MaximumAttempts = 3
	}
	if spec.TimeoutSeconds == 0 {
		spec.TimeoutSeconds = 300
	}
	return spec
}
