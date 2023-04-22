package sort

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func TimeRangeAsc(a, b TimeRange) Bool3VL {
	if a.Start.Before(b.Start) {
		return True3VL
	} else if a.Start.Equal(b.Start) {
		return TimeAsc(a.End, b.End)
	} else {
		return False3VL
	}
}

func TimeRangeDesc(a, b TimeRange) Bool3VL {
	if b.Start.Before(a.Start) {
		return True3VL
	} else if b.Start.Equal(a.Start) {
		return TimeAsc(b.End, a.End)
	} else {
		return False3VL
	}
}
