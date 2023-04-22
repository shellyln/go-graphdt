package cmpr

import (
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

// NOTE: Go>=1.16 The compiler can now inline functions with non-labeled for loops.
// Function should be simple enough, the number of AST nodes must less than the budget (80).

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Swap[T any](fn func(a, b T) bool) func(a, b T) bool {
	return func(a, b T) bool {
		return fn(b, a)
	}
}

func ComparableEq[T comparable](a, b T) bool {
	return a == b
}

func ComparableNotEq[T comparable](a, b T) bool {
	return a != b
}

func OrderedLt[T Ordered](a, b T) bool {
	return a < b
}

func OrderedLe[T Ordered](a, b T) bool {
	return a <= b
}

func ComplexLt[T Complex](a, b T) bool {
	return false
}

func ComplexLe[T Complex](a, b T) bool {
	return a == b
}

func BoolLt[T ~bool](a, b T) T {
	return !a && b
}

func BoolLe[T ~bool](a, b T) T {
	return a == b || !a && b
}

func TimeEq(a, b time.Time) bool {
	return a.Equal(b)
}

func TimeNotEq(a, b time.Time) bool {
	return !a.Equal(b)
}

func TimeLt(a, b time.Time) bool {
	return a.Before(b)
}

func TimeLe(a, b time.Time) bool {
	return a.Equal(b) || a.Before(b)
}

func TimeRangeEq(a, b TimeRange) bool {
	return a.Start.Equal(b.Start) && a.End.Equal(b.End)
}

func TimeRangeNotEq(a, b TimeRange) bool {
	return !a.Start.Equal(b.Start) || !a.End.Equal(b.End)
}

func TimeRangeLt(a, b TimeRange) bool {
	if a.Start.Before(b.Start) {
		return true
	} else if a.Start.Equal(b.Start) {
		return a.End.Before(b.End)
	} else {
		return false
	}
}

func TimeRangeLe(a, b TimeRange) bool {
	if a.Start.Before(b.Start) {
		return true
	} else if a.Start.Equal(b.Start) {
		return a.End.Equal(b.End) || a.End.Before(b.End)
	} else {
		return false
	}
}

func BlobEq[T ~[]byte](a, b T) bool {
	lenA := len(a)
	lenB := len(b)
	if lenA != lenB {
		return false
	}
	for i := 0; i < lenA; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BlobNotEq[T ~[]byte](a, b T) bool {
	lenA := len(a)
	lenB := len(b)
	if lenA != lenB {
		return false
	}
	for i := 0; i < lenA; i++ {
		if a[i] == b[i] {
			return false
		}
	}
	return true
}

func BlobLt[T ~[]byte](a, b T) bool {
	length := min(len(a), len(b))
	for i := 0; i < length; i++ {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}

func BlobLe[T ~[]byte](a, b T) bool {
	length := min(len(a), len(b))
	for i := 0; i < length; i++ {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return true
}
