package scalar

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func Concat(
	rt *runtime.Runtime, argsInfo []runtime.ArgInfo) (
	runtime.HostFunction, types.DataColumnType, error) {

	if len(argsInfo) != 2 {
		return nil, types.Type_Invalid, errors.New("error!")
	}

	cast1 := cast.GetCastFunc(argsInfo[0].Type, types.Type_Nullable_String)
	cast2 := cast.GetCastFunc(argsInfo[1].Type, types.Type_Nullable_String)

	if cast1 == nil || cast2 == nil {
		return nil, types.Type_Invalid, errors.New("error!")
	}

	return func(i int, args ...interface{}) (interface{}, error) {
		cv1, ok := cast1(args[0])
		if !ok {
			return nil, errors.New("error!")
		}
		cv2, ok := cast2(args[1])
		if !ok {
			return nil, errors.New("error!")
		}

		tv1 := cv1.(types.Nullable[string])
		tv2 := cv2.(types.Nullable[string])

		var ret types.Nullable[string]

		if tv1.Valid && tv2.Valid {
			ret.Valid = true
			ret.Value = tv1.Value + tv2.Value
		}

		return ret, nil

	}, types.Type_Nullable_String, nil
}
