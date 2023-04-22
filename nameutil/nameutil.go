package nameutil

import (
	"encoding/base64"
	"strings"
)

// MakeDottedKey returns a dotted key from the given name.
func MakeDottedKeyIgnoreCase(name []string, length int) string {
	var keySb strings.Builder
	for i := 0; i < length; i++ {
		if i != 0 {
			keySb.WriteRune('.')
		}

		x := base64.StdEncoding.EncodeToString([]byte(strings.ToLower(name[i])))
		keySb.WriteString(x)
	}
	return keySb.String()
}

// NameEquals returns true if the two names with namespace are equal, ignoring case.
func NameEqualsIgnoreCase(nameA, nameB []string) bool {
	if len(nameA) != len(nameB) {
		return false
	}
	for i := 0; i < len(nameA); i++ {
		if !strings.EqualFold(nameA[i], nameB[i]) {
			return false
		}
	}
	return true
}

// IsValidName checks if the name is valid.
func IsValidName(name []string) bool {
	if len(name) == 0 {
		return false
	}
	if len(name) == 1 && name[0] == "" {
		return false
	}
	return true
}

// GetNamespaceFromName gets the namespace from the name.
func GetNamespaceFromName(name []string) []string {
	if name == nil {
		return nil
	}
	if len(name) == 0 {
		return name
	}
	return name[:len(name)-1]
}

// MakeJoinedFieldName makes a new name for the joined columns of the right table.
func MakeJoinedFieldName(leftColName []string, relName string, rightColName []string) []string {
	name := make([]string, 0, len(leftColName)+len(rightColName)+1)

	w := 0
	leftDecl := 0
	if relName != "" {
		leftDecl = 1
	}

	for i := 0; i < len(leftColName)-leftDecl; i++ {
		name = append(name, leftColName[i])
		w++
	}
	if leftDecl > 0 {
		name = append(name, relName)
		w++
	}

	if w > 0 && len(rightColName) > 1 {
		rightColName = rightColName[1:]
	}
	name = append(name, rightColName...)

	return name
}
