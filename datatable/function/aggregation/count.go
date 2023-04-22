package aggregation

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func Count(
	rt *runtime.Runtime, argsInfo []runtime.ArgInfo) (
	runtime.HostFunction, types.DataColumnType, error) {

	if len(argsInfo) != 1 {
		return nil, types.Type_Invalid, errors.New("error!")
	}

	cast1 := cast.GetCastFunc(argsInfo[0].Type, types.Type_Nullable_Any|types.Type_Flag_Array)

	if cast1 == nil {
		return nil, types.Type_Invalid, errors.New("error!")
	}

	return func(i int, args ...interface{}) (interface{}, error) {
		cv1, ok := cast1(args[0])
		if !ok {
			return nil, errors.New("error!")
		}

		tv1 := cv1.([]types.Nullable[interface{}])

		var ret int64

		for _, v := range tv1 {
			if v.Valid {
				ret++
			}
		}

		return ret, nil

	}, types.Type_I64, nil
}
