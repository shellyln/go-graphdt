package sort

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func ComplexAsc[T Complex](a, b T) Bool3VL {
	aa := real(complex128(a)) * imag(complex128(a))
	bb := real(complex128(b)) * imag(complex128(b))
	if aa < bb {
		return True3VL
	} else if aa == bb {
		return Unknown3VL
	} else {
		return False3VL
	}
}

func ComplexDesc[T Complex](a, b T) Bool3VL {
	aa := real(complex128(a)) * imag(complex128(a))
	bb := real(complex128(b)) * imag(complex128(b))
	if bb < aa {
		return True3VL
	} else if aa == bb {
		return Unknown3VL
	} else {
		return False3VL
	}
}
