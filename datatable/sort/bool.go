package sort

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func BoolAsc(a, b bool) Bool3VL {
	if !a && b {
		return True3VL
	} else if a == b {
		return Unknown3VL
	} else {
		return False3VL
	}
}

func BoolDesc(a, b bool) Bool3VL {
	if !b && a {
		return True3VL
	} else if a == b {
		return Unknown3VL
	} else {
		return False3VL
	}
}
