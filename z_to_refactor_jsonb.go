package pg

import (
	"database/sql/driver"
	"encoding/json"
)

func ScanJsonb(dest any) *jsonbScanner {
	return &jsonbScanner{dest: dest}
}

type jsonbScanner struct {
	dest any
}

func (n *jsonbScanner) Scan(value any) error {
	if value == nil {
		n.dest = map[string]any{}
		return nil
	}
	return json.Unmarshal(value.([]byte), n.dest)
}

func Jsonb(value map[string]any) *jsonbValuer {
	return &jsonbValuer{value: value}
}

type jsonbValuer struct {
	value map[string]any
}

// Value implements the driver Valuer interface.
func (n *jsonbValuer) Value() (driver.Value, error) {
	if n.value != nil {
		return json.Marshal(n.value)
	}

	return nil, nil
}
