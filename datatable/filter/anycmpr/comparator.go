package anycmpr

import (
	"reflect"
	"regexp"
	"strings"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func InComparator(cmp func(a, b interface{}) Bool3VL) func(a, b interface{}) Bool3VL {
	return func(cv1, cv2 interface{}) Bool3VL {
		ret := False3VL

		rcv2 := reflect.ValueOf(cv2)
		length := rcv2.Len()
		for i := 0; i < length; i++ {
			c := rcv2.Index(i).Interface()
			ret |= cmp(cv1, c)
			if ret == True3VL {
				return True3VL
			}
		}
		return ret
	}
}

func NotInComparator(cmp func(a, b interface{}) Bool3VL) func(a, b interface{}) Bool3VL {
	return func(cv1, cv2 interface{}) Bool3VL {
		ret := True3VL

		rcv2 := reflect.ValueOf(cv2)
		length := rcv2.Len()
		for i := 0; i < length; i++ {
			c := rcv2.Index(i).Interface()
			r := cmp(cv1, c)
			if r == True3VL {
				return False3VL
			}
			if r == Unknown3VL {
				ret = Unknown3VL
			}
		}
		return ret
	}
}

func IncludesComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(string)
		cs2 := b.([][]string)

	OUTER:
		for _, tv2 := range cs2 {
			s := ";" + string(tv1) + ";"
			for _, needle := range tv2 {
				if !strings.Contains(s, needle) {
					continue OUTER
				}
			}
			return True3VL
		}
		return False3VL
	}
}

func Nullable2VLIncludesComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		ret := False3VL
		tv1 := a.(Nullable[string])
		cs2 := b.([]Nullable[[]string])

	OUTER:
		for _, tv2 := range cs2 {
			if tv1.Valid && tv2.Valid {
				s := ";" + string(tv1.Value) + ";"
				for _, needle := range tv2.Value {
					if !strings.Contains(s, needle) {
						continue OUTER
					}
				}
				return True3VL
			} else {
				ret = Unknown3VL // TODO: 3VL/2VL
			}
		}
		return ret
	}
}

func Nullable3VLIncludesComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		ret := False3VL
		tv1 := a.(Nullable[string])
		cs2 := b.([]Nullable[[]string])

	OUTER:
		for _, tv2 := range cs2 {
			if tv1.Valid && tv2.Valid {
				s := ";" + string(tv1.Value) + ";"
				for _, needle := range tv2.Value {
					if !strings.Contains(s, needle) {
						continue OUTER
					}
				}
				return True3VL
			} else {
				ret = Unknown3VL // TODO: 3VL/2VL
			}
		}
		return ret
	}
}

func ExcludesComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(string)
		cs2 := b.([][]string)

	OUTER:
		for _, tv2 := range cs2 {
			s := ";" + string(tv1) + ";"
			for _, needle := range tv2 {
				if !strings.Contains(s, needle) {
					continue OUTER
				}
			}
			return False3VL
		}
		return True3VL
	}
}

func Nullable2VLExcludesComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		ret := True3VL
		tv1 := a.(Nullable[string])
		cs2 := b.([]Nullable[[]string])

	OUTER:
		for _, tv2 := range cs2 {
			if tv1.Valid && tv2.Valid {
				s := ";" + string(tv1.Value) + ";"
				for _, needle := range tv2.Value {
					if !strings.Contains(s, needle) {
						continue OUTER
					}
				}
				return False3VL
			} else {
				ret = Unknown3VL // TODO: 3VL/2VL
			}
		}
		return ret
	}
}

func Nullable3VLExcludesComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		ret := True3VL
		tv1 := a.(Nullable[string])
		cs2 := b.([]Nullable[[]string])

	OUTER:
		for _, tv2 := range cs2 {
			if tv1.Valid && tv2.Valid {
				s := ";" + string(tv1.Value) + ";"
				for _, needle := range tv2.Value {
					if !strings.Contains(s, needle) {
						continue OUTER
					}
				}
				return False3VL
			} else {
				ret = Unknown3VL // TODO: 3VL/2VL
			}
		}
		return ret
	}
}

func LikeComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(string)
		tv2 := b.(*regexp.Regexp)

		if tv2.Match([]byte(tv1)) {
			return True3VL
		} else {
			return False3VL
		}
	}
}

func Nullable2VLLikeComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return True3VL
			} else {
				return False3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}

func Nullable3VLLikeComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return True3VL
			} else {
				return False3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}

func NotLikeComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(string)
		tv2 := b.(*regexp.Regexp)

		if tv2.Match([]byte(tv1)) {
			return False3VL
		} else {
			return True3VL
		}
	}
}

func Nullable2VLNotLikeComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return False3VL
			} else {
				return True3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}

func Nullable3VLNotLikeComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return False3VL
			} else {
				return True3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}

func MatchComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(string)
		tv2 := b.(*regexp.Regexp)

		if tv2.Match([]byte(tv1)) {
			return True3VL
		} else {
			return False3VL
		}
	}
}

func Nullable2VLMatchComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return True3VL
			} else {
				return False3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}

func Nullable3VLMatchComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return True3VL
			} else {
				return False3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}

func NotMatchComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(string)
		tv2 := b.(*regexp.Regexp)

		if tv2.Match([]byte(tv1)) {
			return False3VL
		} else {
			return True3VL
		}
	}
}

func Nullable2VLNotMatchComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return False3VL
			} else {
				return True3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}

func Nullable3VLNotMatchComparator() func(a, b interface{}) Bool3VL {
	return func(a, b interface{}) Bool3VL {
		tv1 := a.(Nullable[string])
		tv2 := b.(Nullable[*regexp.Regexp])

		if tv1.Valid && tv2.Valid {
			if tv2.Value.Match([]byte(tv1.Value)) {
				return False3VL
			} else {
				return True3VL
			}
		}
		return Unknown3VL // TODO: 3VL/2VL
	}
}
