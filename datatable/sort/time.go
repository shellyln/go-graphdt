package sort

import (
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func TimeAsc(a, b time.Time) Bool3VL {
	if a.Before(b) {
		return True3VL
	} else if a.Equal(b) {
		return Unknown3VL
	} else {
		return False3VL
	}
}

func TimeDesc(a, b time.Time) Bool3VL {
	if b.Before(a) {
		return True3VL
	} else if a.Equal(b) {
		return Unknown3VL
	} else {
		return False3VL
	}
}
