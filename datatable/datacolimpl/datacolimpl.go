package datacolimpl

import (
	"errors"
	"fmt"
	"sort"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

const (
	DataColumnImpl_DefaultSize = 1024
	DataColumnImpl_AlignSize   = 256
	DataColumnImpl_GrowRate    = 1.25
)

type DataColumnImpl[T any] struct {
	typ       DataColumnType
	borrowed  bool
	checkNull func(v T) bool
	castElem  func(v interface{}) (r T, ok bool)
	castArray func(v interface{}) (r []T, ok bool)
	values    []T
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) IsOwned() bool {
	return !p.borrowed
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Own() {
	if p.borrowed {
		*p = *(p.Copy())
	}
}

func (p *DataColumnImpl[T]) Borrow() *DataColumnImpl[T] {
	return &DataColumnImpl[T]{
		borrowed:  true,
		typ:       p.typ,
		checkNull: p.checkNull,
		castElem:  p.castElem,
		castArray: p.castArray,
		values:    p.values,
	}
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) GetType() DataColumnType {
	return p.typ
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) GetImpl() AnyDataColumn {
	return p
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Len() int {
	return len(p.values)
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Cap() int {
	return cap(p.values)
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Resize(n int) {
	// TODO: allocation strategy
	if !p.borrowed && n <= cap(p.values) {
		p.values = p.values[:n]
	} else {
		c := cap(p.values)
		if cap(p.values) < n {
			c = n
			if c < DataColumnImpl_DefaultSize {
				c = DataColumnImpl_AlignSize * ((c*2)/DataColumnImpl_AlignSize + 1)
			} else {
				c = DataColumnImpl_AlignSize * (int(float64(c)*DataColumnImpl_GrowRate)/DataColumnImpl_AlignSize + 1)
			}
		}
		s := make([]T, n, c)
		copy(s, p.values)
		p.borrowed = false
		p.values = s
	}
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Grow(n int) {
	// TODO: allocation strategy
	if !p.borrowed && n <= cap(p.values) {
		p.values = p.values[:n]
	} else {
		c := cap(p.values)
		if cap(p.values) < n {
			c = n
			if c < DataColumnImpl_DefaultSize {
				c = DataColumnImpl_AlignSize * ((c*2)/DataColumnImpl_AlignSize + 1)
			} else {
				c = DataColumnImpl_AlignSize * (int(float64(c)*DataColumnImpl_GrowRate)/DataColumnImpl_AlignSize + 1)
			}
		}
		s := make([]T, n, c)
		copy(s, p.values)
		p.borrowed = false
		p.values = s
	}
}

func (p *DataColumnImpl[T]) IsNull(i int) bool {
	return p.checkNull(p.values[i])
}

func (p *DataColumnImpl[T]) Cast(v interface{}) (T, bool) {
	return p.castElem(v)
}

func (p *DataColumnImpl[T]) CastArray(v interface{}) ([]T, bool) {
	return p.castArray(v)
}

func (p *DataColumnImpl[T]) Get(i int) T {
	return p.values[i]
}

func (p *DataColumnImpl[T]) GetRawValues() []T {
	return p.values
}

func (p *DataColumnImpl[T]) SetRawValues(values []T) {
	p.values = values
	p.borrowed = false
}

func (p *DataColumnImpl[T]) Set(i int, v T) {
	p.values[i] = v
}

func (p *DataColumnImpl[T]) Fill(s, e int, v T) {
	for i := s; i < e; i++ {
		p.values[i] = v
	}
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Sort(less func(a, b int) bool) {
	values := p.values
	sort.SliceStable(values, less)
}

func (p *DataColumnImpl[T]) ApplySortFunc(less func(a, b T) Bool3VL) SortFunc {
	// DON'T cache the `p.values`
	return func(i, j int) Bool3VL {
		return less(p.values[i], p.values[j])
	}
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Reverse() {
	length := len(p.values)
	half := length / 2
	length_1 := length - 1
	s := p.values

	for i := 0; i < half; i++ {
		v := s[i]
		s[i] = s[length_1-i]
		s[length_1-i] = v
	}
}

func (p *DataColumnImpl[T]) Copy() *DataColumnImpl[T] {
	dst := make([]T, len(p.values))
	copy(dst, p.values)

	return &DataColumnImpl[T]{
		typ:       p.typ,
		checkNull: p.checkNull,
		castElem:  p.castElem,
		castArray: p.castArray,
		values:    dst,
	}
}

func (p *DataColumnImpl[T]) Slice(offset, limit int) *DataColumnImpl[T] {
	s := offset
	e := offset + limit
	if s < 0 {
		s = 0
	}
	if e > len(p.values) {
		e = len(p.values)
	}

	return &DataColumnImpl[T]{
		typ:       p.typ,
		borrowed:  true,
		checkNull: p.checkNull,
		castElem:  p.castElem,
		castArray: p.castArray,
		values:    p.values[s:e],
	}
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) For(iter EnumeratorFunc) {
	length := len(p.values)
	for i := 0; i < length; i++ {
		iter(i)
	}
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) Filter(filter FilterFunc, iter EnumeratorFunc) {
	length := len(p.values)
	for i := 0; i < length; i++ {
		if filter(i) {
			iter(i)
		}
	}
}

func (p *DataColumnImpl[T]) ApplyFilterFunc(filter func(v T) Bool3VL) Filter3VLFunc {
	// DON'T cache `p.values`
	return func(i int) Bool3VL {
		return filter(p.values[i])
	}
}

func (p *DataColumnImpl[T]) Append(values ...T) {
	if p.borrowed {
		dst := make([]T, 0, len(p.values)+len(values))
		dst = append(dst, p.values...)
		dst = append(dst, values...)
		p.borrowed = false
		p.values = dst
	} else {
		p.values = append(p.values, values...)
	}
}

func (p *DataColumnImpl[T]) AppendDataColumn(dc *DataColumnImpl[T]) {
	p.Append(dc.values...)
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) AppendAnyDataColumn(dc AnyDataColumn) error {
	val, ok := dc.(*DataColumnImpl[T])
	if !ok {
		return fmt.Errorf("Cannot convert type: %v", dc)
	}

	p.AppendDataColumn(val)
	return nil
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) GetFilter2VL(op FilterOp, v interface{}) (FilterGenFunc, error) {
	return nil, errors.New("DataColumnImpl: GetFilter2VL: Not impl")
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) GetFilter3VL(op FilterOp, v interface{}) (FilterGenFunc, error) {
	return nil, errors.New("DataColumnImpl: GetFilter3VL: Not impl")
}

// interface AnyDataColumn
func (p *DataColumnImpl[T]) GetSort(desc bool, nullsLast bool) SortFunc {
	return nil
}
