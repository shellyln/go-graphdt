package sort

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func TAscNullsFirst[T any, F func(a, b T) Bool3VL](asc F) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			return asc(a.Value, b.Value)
		}
		if a.Valid == b.Valid {
			return Unknown3VL
		}
		if !a.Valid {
			return True3VL
		}
		return False3VL
	}
}

func TDescNullsFirst[T any, F func(a, b T) Bool3VL](desc F) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			return desc(a.Value, b.Value)
		}
		if a.Valid == b.Valid {
			return Unknown3VL
		}
		if !b.Valid {
			return True3VL
		}
		return False3VL
	}
}

func TAscNullsLast[T any, F func(a, b T) Bool3VL](asc F) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			return asc(a.Value, b.Value)
		}
		if a.Valid == b.Valid {
			return Unknown3VL
		}
		if !a.Valid {
			return False3VL
		}
		return True3VL
	}
}

func TDescNullsLast[T any, F func(a, b T) Bool3VL](desc F) func(a, b Nullable[T]) Bool3VL {
	return func(a, b Nullable[T]) Bool3VL {
		if a.Valid && b.Valid {
			return desc(a.Value, b.Value)
		}
		if a.Valid == b.Valid {
			return Unknown3VL
		}
		if !b.Valid {
			return False3VL
		}
		return True3VL
	}
}
