package types

import (
	"encoding/json"
	"fmt"
)

func (t Nullable[T]) MarshalJSON() ([]byte, error) {
	if t.Valid {
		return json.Marshal(t.Value)
	} else {
		return []byte(`null`), nil
	}
}

func (t *Nullable[T]) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" {
		var zero T
		t.Valid = false
		t.Value = zero
		return nil
	} else {
		t.Valid = true
		return json.Unmarshal(b, &t.Value)
	}
}

func (t Nullable[T]) String() string {
	if t.Valid {
		return fmt.Sprint(t.Value)
	}
	return "(null)"
}
