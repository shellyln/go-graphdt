package sort

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func BlobAsc(a, b []byte) Bool3VL {
	length := min(len(a), len(b))
	for i := 0; i < length; i++ {
		if a[i] < b[i] {
			return True3VL
		} else if a[i] > b[i] {
			return False3VL
		}
	}
	return Unknown3VL
}

func BlobDesc(a, b []byte) Bool3VL {
	length := min(len(a), len(b))
	for i := 0; i < length; i++ {
		if b[i] < a[i] {
			return True3VL
		} else if b[i] > a[i] {
			return False3VL
		}
	}
	return Unknown3VL
}
