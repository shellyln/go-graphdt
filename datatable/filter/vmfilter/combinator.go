package vmfilter

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func unaryCmpFuncOf[T any](fn func(a T) Bool3VL) func(a interface{}) Bool3VL {
	return func(a interface{}) Bool3VL {
		return fn(a.(T))
	}
}

func binaryCmpFuncOf[T any](fn func(a, b T) Bool3VL) func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		return fn(a.(T), b.(T))
	}
}

func fromComparator[T any](fn func(a, b T) bool) func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		if fn(a.(T), b.(T)) {
			return True3VL
		} else {
			return False3VL
		}
	}
}

func UnaryTrue(a interface{}) Bool3VL {
	return True3VL
}

func UnaryFalse(a interface{}) Bool3VL {
	return False3VL
}

func UnaryUnknown(a interface{}) Bool3VL {
	return Unknown3VL
}

func BinaryTrue(a, b interface{}) Bool3VL {
	return True3VL
}

func BinaryFalse(a, b interface{}) Bool3VL {
	return False3VL
}

func BinaryUnknown(a, b interface{}) Bool3VL {
	return Unknown3VL
}

func Nullable2VL[T any](cmpEq func(a, b T) bool) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			if cmpEq(a.Value, b.Value) {
				return True3VL
			} else {
				return False3VL
			}
		}
		if a.Valid == b.Valid {
			return True3VL
		} else {
			return False3VL
		}
	}
}

func Nullable2VLNot[T any](cmpEq func(a, b T) bool) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			if !cmpEq(a.Value, b.Value) {
				return True3VL
			} else {
				return False3VL
			}
		}
		if a.Valid != b.Valid {
			return True3VL
		} else {
			return False3VL
		}
	}
}

func Nullable3VL[T any](cmpEq func(a, b T) bool) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			if cmpEq(a.Value, b.Value) {
				return True3VL
			} else {
				return False3VL
			}
		}
		return Unknown3VL
	}
}

func Nullable3VLNot[T any](cmpEq func(a, b T) bool) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			if !cmpEq(a.Value, b.Value) {
				return True3VL
			} else {
				return False3VL
			}
		}
		return Unknown3VL
	}
}

func Nullable3VLIsNull[T any](a Nullable[T]) Bool3VL {
	if !a.Valid {
		return True3VL
	} else {
		return False3VL
	}
}

func Nullable3VLIsNotNull[T any](a Nullable[T]) Bool3VL {
	if a.Valid {
		return True3VL
	} else {
		return False3VL
	}
}
