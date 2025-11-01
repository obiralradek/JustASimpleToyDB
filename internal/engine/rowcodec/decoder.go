package rowcodec

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"justasimpletoydb/internal/catalog"
)

func DecodeRow(schema *catalog.TableSchema, data []byte) ([]any, error) {
	buf := bytes.NewReader(data)
	result := make([]any, len(schema.Columns))

	for i, col := range schema.Columns {
		switch col.Type {
		case catalog.TypeInt:
			var v uint64
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("decode int for %s: %v", col.Name, err)
			}
			result[i] = int(v)

		case catalog.TypeText:
			var length uint32
			if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
				return nil, fmt.Errorf("decode length for %s: %v", col.Name, err)
			}
			strBytes := make([]byte, length)
			if _, err := buf.Read(strBytes); err != nil {
				return nil, fmt.Errorf("decode text for %s: %v", col.Name, err)
			}
			result[i] = string(strBytes)

		default:
			return nil, fmt.Errorf("unsupported type for column %s", col.Name)
		}
	}

	return result, nil
}
