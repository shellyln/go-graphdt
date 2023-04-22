package datacolimpl

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func NewDataColumnImpl[T any](ty DataColumnType) *DataColumnImpl[T] {
	return &DataColumnImpl[T]{
		typ:       ty,
		checkNull: dummyCheckNull[T],
		castElem:  dummyCastElem[T],
		castArray: dummyCastArray[T],
		values:    make([]T, 0, DataColumnImpl_DefaultSize),
	}
}

func NewDataColumnImplWithSize[T any](l, c int, ty DataColumnType) *DataColumnImpl[T] {
	return &DataColumnImpl[T]{
		typ:       ty,
		checkNull: dummyCheckNull[T],
		castElem:  dummyCastElem[T],
		castArray: dummyCastArray[T],
		values:    make([]T, l, c),
	}
}

func (p *DataColumnImpl[T]) SetCheckNull(checkNull func(v T) bool) {
	p.checkNull = checkNull
}

func (p *DataColumnImpl[T]) SetCast(castElem func(v interface{}) (r T, ok bool), castArray func(v interface{}) (r []T, ok bool)) {
	p.castElem = castElem
	p.castArray = castArray
}
