package datacolimpl

import (
	"fmt"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

// interface AnyDataColumn
func (p *DataColumnImpl[T]) BorrowAsAny() AnyDataColumn {
	return p.Borrow()
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) CastAsAny(v interface{}) (interface{}, bool) {
	return p.castElem(v)
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) CastArrayAsAny(v interface{}) (interface{}, bool) {
	return p.castArray(v)
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) GetAny(i int) interface{} {
	return p.values[i]
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) SetAny(i int, v interface{}) error {
	val, ok := p.castElem(v)
	if !ok {
		return fmt.Errorf("Cannot convert type: %v", v)
	}

	p.values[i] = val
	return nil
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) SetAnyNoCast(i int, v interface{}) error {
	val, ok := v.(T)
	if !ok {
		return fmt.Errorf("Cannot convert type: %v", v)
	}

	p.values[i] = val
	return nil
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) FillAny(s, e int, v interface{}) error {
	val, ok := p.castElem(v)
	if !ok {
		return fmt.Errorf("Cannot convert type: %v", v)
	}

	for i := s; i < e; i++ {
		p.values[i] = val
	}
	return nil
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) GetRawValuesAsAny() interface{} {
	return p.values
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) CopyAsAny() AnyDataColumn {
	return p.Copy()
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) SliceAsAny(offset, limit int) AnyDataColumn {
	return p.Slice(offset, limit)
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) AppendAny(values ...interface{}) error {
	var dst []T
	if p.borrowed || len(p.values)+len(values) > cap(p.values) {
		dst = make([]T, 0, len(p.values)+len(values))
		dst = append(dst, p.values...)
	} else {
		dst = p.values
	}

	length := len(values)
	for i := 0; i < length; i++ {
		val, ok := p.castElem(values[i])
		if !ok {
			return fmt.Errorf("Cannot convert type: %v", values[i])
		}
		dst = append(dst, val)
	}

	p.borrowed = false
	p.values = dst
	return nil
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) MakeBufferAsAny(c int) interface{} {
	return make([]T, 0, c)
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) CopyBufferByIndex(buf *interface{}, index []int) {
	dst := (*buf).([]T)[:len(index)]
	for i, v := range index {
		dst[i] = p.values[v]
	}
	*buf = dst
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) FillByRowMap(dstRowMap []int, src AnyDataColumn, srcRowMap []int) {
	rawSrc := src.GetRawValuesAsAny().([]T)
	rawDst := p.values
	if dstRowMap != nil {
		for i, v := range srcRowMap {
			rawDst[dstRowMap[i]] = rawSrc[v]
		}
	} else {
		for i, v := range srcRowMap {
			rawDst[i] = rawSrc[v]
		}
	}
}
