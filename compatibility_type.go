package schemakeepergo

import (
	"strings"
)

type CompatibilityType string

const (
	None               CompatibilityType = "none"
	Backward           CompatibilityType = "backward"
	Forward            CompatibilityType = "forward"
	Full               CompatibilityType = "full"
	BackwardTransitive CompatibilityType = "backward_transitive"
	ForwardTransitive  CompatibilityType = "forward_transitive"
	FullTransitive     CompatibilityType = "full_transitive"
)

func CompatibilityTypeByName(value string) CompatibilityType {
	name := strings.ToLower(value)

	switch name {
	case "none": return None
	case "backward": return Backward
	case "forward": return Forward
	case "full": return Full
	case "backward_transitive": return BackwardTransitive
	case "forward_transitive": return ForwardTransitive
	case "full_transitive": return FullTransitive
	default: return Backward //todo: log incorrect compatibilityType name
	}
}
