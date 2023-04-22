package datacolumn

import (
	"time"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/sort"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// interface AnyDataColumn
func (p *DataColumn) GetSort(desc bool, nullsLast bool) SortFunc {
	var fn SortFunc
	switch p.typ {
	case Type_I8:
		col := p.col.(*datacolimpl.DataColumnImpl[int8])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[int8])
		} else {
			fn = col.ApplySortFunc(sort.Asc[int8])
		}
	case Type_I16:
		col := p.col.(*datacolimpl.DataColumnImpl[int16])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[int16])
		} else {
			fn = col.ApplySortFunc(sort.Asc[int16])
		}
	case Type_I32:
		col := p.col.(*datacolimpl.DataColumnImpl[int32])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[int32])
		} else {
			fn = col.ApplySortFunc(sort.Asc[int32])
		}
	case Type_I64:
		col := p.col.(*datacolimpl.DataColumnImpl[int64])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[int64])
		} else {
			fn = col.ApplySortFunc(sort.Asc[int64])
		}
	case Type_Int:
		col := p.col.(*datacolimpl.DataColumnImpl[int])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[int])
		} else {
			fn = col.ApplySortFunc(sort.Asc[int])
		}
	case Type_U8:
		col := p.col.(*datacolimpl.DataColumnImpl[uint8])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[uint8])
		} else {
			fn = col.ApplySortFunc(sort.Asc[uint8])
		}
	case Type_U16:
		col := p.col.(*datacolimpl.DataColumnImpl[uint16])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[uint16])
		} else {
			fn = col.ApplySortFunc(sort.Asc[uint16])
		}
	case Type_U32:
		col := p.col.(*datacolimpl.DataColumnImpl[uint32])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[uint32])
		} else {
			fn = col.ApplySortFunc(sort.Asc[uint32])
		}
	case Type_U64:
		col := p.col.(*datacolimpl.DataColumnImpl[uint64])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[uint64])
		} else {
			fn = col.ApplySortFunc(sort.Asc[uint64])
		}
	case Type_Uint:
		col := p.col.(*datacolimpl.DataColumnImpl[uint])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[uint])
		} else {
			fn = col.ApplySortFunc(sort.Asc[uint])
		}
	case Type_UintPtr:
		col := p.col.(*datacolimpl.DataColumnImpl[uintptr])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[uintptr])
		} else {
			fn = col.ApplySortFunc(sort.Asc[uintptr])
		}
	case Type_F32:
		col := p.col.(*datacolimpl.DataColumnImpl[float32])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[float32])
		} else {
			fn = col.ApplySortFunc(sort.Asc[float32])
		}
	case Type_F64:
		col := p.col.(*datacolimpl.DataColumnImpl[float64])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[float64])
		} else {
			fn = col.ApplySortFunc(sort.Asc[float64])
		}
	case Type_Complex64:
		col := p.col.(*datacolimpl.DataColumnImpl[complex64])
		if desc {
			fn = col.ApplySortFunc(sort.ComplexDesc[complex64])
		} else {
			fn = col.ApplySortFunc(sort.ComplexDesc[complex64])
		}
	case Type_Complex128:
		col := p.col.(*datacolimpl.DataColumnImpl[complex128])
		if desc {
			fn = col.ApplySortFunc(sort.ComplexDesc[complex128])
		} else {
			fn = col.ApplySortFunc(sort.ComplexDesc[complex128])
		}
	case Type_Bool:
		col := p.col.(*datacolimpl.DataColumnImpl[bool])
		if desc {
			fn = col.ApplySortFunc(sort.BoolDesc)
		} else {
			fn = col.ApplySortFunc(sort.BoolAsc)
		}
	case Type_String:
		col := p.col.(*datacolimpl.DataColumnImpl[string])
		if desc {
			fn = col.ApplySortFunc(sort.Desc[string])
		} else {
			fn = col.ApplySortFunc(sort.Asc[string])
		}
	case Type_Blob:
		col := p.col.(*datacolimpl.DataColumnImpl[[]byte])
		if desc {
			fn = col.ApplySortFunc(sort.BlobDesc)
		} else {
			fn = col.ApplySortFunc(sort.BlobAsc)
		}
	case Type_DateTime:
		col := p.col.(*datacolimpl.DataColumnImpl[time.Time])
		if desc {
			fn = col.ApplySortFunc(sort.TimeDesc)
		} else {
			fn = col.ApplySortFunc(sort.TimeAsc)
		}
	case Type_DateTimeRange:
		col := p.col.(*datacolimpl.DataColumnImpl[TimeRange])
		if desc {
			fn = col.ApplySortFunc(sort.TimeRangeDesc)
		} else {
			fn = col.ApplySortFunc(sort.TimeRangeAsc)
		}
	case Type_Any:
		if desc {
			fn = sort.Dummy
		} else {
			fn = sort.Dummy
		}
	case Type_Nullable_I8:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[int8]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[int8]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[int8]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[int8]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[int8]))
			}
		}
	case Type_Nullable_I16:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[int16]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[int16]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[int16]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[int16]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[int16]))
			}
		}
	case Type_Nullable_I32:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[int32]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[int32]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[int32]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[int32]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[int32]))
			}
		}
	case Type_Nullable_I64:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[int64]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[int64]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[int64]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[int64]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[int64]))
			}
		}
	case Type_Nullable_Int:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[int]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[int]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[int]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[int]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[int]))
			}
		}
	case Type_Nullable_U8:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[uint8]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[uint8]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[uint8]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[uint8]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[uint8]))
			}
		}
	case Type_Nullable_U16:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[uint16]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[uint16]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[uint16]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[uint16]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[uint16]))
			}
		}
	case Type_Nullable_U32:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[uint32]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[uint32]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[uint32]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[uint32]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[uint32]))
			}
		}
	case Type_Nullable_U64:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[uint64]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[uint64]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[uint64]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[uint64]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[uint64]))
			}
		}
	case Type_Nullable_Uint:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[uint]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[uint]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[uint]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[uint]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[uint]))
			}
		}
	case Type_Nullable_UintPtr:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[uintptr]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[uintptr]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[uintptr]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[uintptr]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[uintptr]))
			}
		}
	case Type_Nullable_F32:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[float32]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[float32]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[float32]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[float32]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[float32]))
			}
		}
	case Type_Nullable_F64:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[float64]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[float64]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[float64]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[float64]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[float64]))
			}
		}
	case Type_Nullable_Complex64:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[complex64]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.ComplexDesc[complex64]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.ComplexDesc[complex64]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.ComplexAsc[complex64]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.ComplexAsc[complex64]))
			}
		}
	case Type_Nullable_Complex128:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[complex128]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.ComplexDesc[complex128]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.ComplexDesc[complex128]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.ComplexAsc[complex128]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.ComplexAsc[complex128]))
			}
		}
	case Type_Nullable_Bool:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[bool]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.BoolDesc))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.BoolDesc))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.BoolAsc))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.BoolAsc))
			}
		}
	case Type_Nullable_String:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[string]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.Desc[string]))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.Desc[string]))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.Asc[string]))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.Asc[string]))
			}
		}
	case Type_Nullable_Blob:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[[]byte]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.BlobDesc))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.BlobDesc))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.BlobAsc))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.BlobAsc))
			}
		}
	case Type_Nullable_DateTime:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[time.Time]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.TimeDesc))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.TimeDesc))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.TimeAsc))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.TimeAsc))
			}
		}
	case Type_Nullable_DateTimeRange:
		col := p.col.(*datacolimpl.DataColumnImpl[Nullable[TimeRange]])
		if desc {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TDescNullsLast(sort.TimeRangeDesc))
			} else {
				fn = col.ApplySortFunc(sort.TDescNullsFirst(sort.TimeRangeDesc))
			}
		} else {
			if nullsLast {
				fn = col.ApplySortFunc(sort.TAscNullsLast(sort.TimeRangeAsc))
			} else {
				fn = col.ApplySortFunc(sort.TAscNullsFirst(sort.TimeRangeAsc))
			}
		}
	case Type_Nullable_Any:
		if desc {
			fn = sort.Dummy
		} else {
			fn = sort.Dummy
		}
	}
	return fn
}
