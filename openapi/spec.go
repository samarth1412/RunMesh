package openapi

import _ "embed"

// Spec is the public API contract served by the control plane.
//
//go:embed openapi.yaml
var Spec []byte
