package rowcodec

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"justasimpletoydb/internal/catalog"
)

func EncodeRow(schema *catalog.TableSchema, values []any) ([]byte, error) {
	if len(values) != len(schema.Columns) {
		return nil, fmt.Errorf("expected %d values, got %d", len(schema.Columns), len(values))
	}

	buf := &bytes.Buffer{}

	for i, col := range schema.Columns {
		switch col.Type {
		case catalog.TypeInt:
			v, ok := values[i].(int)
			if !ok {
				return nil, fmt.Errorf("column %s expects int", col.Name)
			}
			tmp := make([]byte, 8)
			binary.LittleEndian.PutUint64(tmp, uint64(v))
			buf.Write(tmp)

		case catalog.TypeText:
			v, ok := values[i].(string)
			if !ok {
				return nil, fmt.Errorf("column %s expects string", col.Name)
			}
			length := uint32(len(v))
			binary.Write(buf, binary.LittleEndian, length)
			buf.WriteString(v)

		default:
			return nil, fmt.Errorf("unsupported type for column %s", col.Name)
		}
	}

	return buf.Bytes(), nil
}
