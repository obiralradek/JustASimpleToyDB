package rowcodec

import (
	"justasimpletoydb/internal/catalog"
	"testing"
)

func TestDecodeRow_IntOnly(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	// Encode first
	values := []any{42}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Decode
	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(decoded))
	}

	if decoded[0].(int) != 42 {
		t.Errorf("Expected 42, got %v", decoded[0])
	}
}

func TestDecodeRow_TextOnly(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "name", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{"hello"}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(decoded))
	}

	if decoded[0].(string) != "hello" {
		t.Errorf("Expected 'hello', got %v", decoded[0])
	}
}

func TestDecodeRow_MixedTypes(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
			{Name: "age", Type: catalog.TypeInt},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{1, "Alice", 30}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(decoded) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(decoded))
	}

	if decoded[0].(int) != 1 {
		t.Errorf("Expected id=1, got %v", decoded[0])
	}

	if decoded[1].(string) != "Alice" {
		t.Errorf("Expected name='Alice', got %v", decoded[1])
	}

	if decoded[2].(int) != 30 {
		t.Errorf("Expected age=30, got %v", decoded[2])
	}
}

func TestDecodeRow_RoundTrip(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
			{Name: "active", Type: catalog.TypeInt},
			{Name: "email", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	testCases := [][]any{
		{1, "Alice", 1, "alice@example.com"},
		{2, "Bob", 0, "bob@example.com"},
		{3, "Charlie", 1, "charlie@example.com"},
		{0, "", 0, ""},
		{-1, "Negative ID", 1, "test@test.com"},
		{9223372036854775807, "Max Int", 0, "max@example.com"},
	}

	for i, values := range testCases {
		encoded, err := EncodeRow(schema, values)
		if err != nil {
			t.Fatalf("Test case %d: Failed to encode: %v", i, err)
		}

		decoded, err := DecodeRow(schema, encoded)
		if err != nil {
			t.Fatalf("Test case %d: Failed to decode: %v", i, err)
		}

		if len(decoded) != len(values) {
			t.Fatalf("Test case %d: Expected %d values, got %d", i, len(values), len(decoded))
		}

		for j, expected := range values {
			if decoded[j] != expected {
				t.Errorf("Test case %d, value %d: Expected %v, got %v", i, j, expected, decoded[j])
			}
		}
	}
}

func TestDecodeRow_EmptyString(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "name", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{""}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded[0].(string) != "" {
		t.Errorf("Expected empty string, got %v", decoded[0])
	}
}

func TestDecodeRow_LongString(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "text", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	longString := make([]byte, 1000)
	for i := range longString {
		longString[i] = 'a'
	}

	values := []any{string(longString)}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded[0].(string) != string(longString) {
		t.Error("Decoded long string doesn't match original")
	}
}

func TestDecodeRow_NegativeInt(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{-42}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded[0].(int) != -42 {
		t.Errorf("Expected -42, got %v", decoded[0])
	}
}

func TestDecodeRow_LargeInt(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{9223372036854775807} // Max int64
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded[0].(int) != 9223372036854775807 {
		t.Errorf("Expected max int64, got %v", decoded[0])
	}
}

func TestDecodeRow_MultipleTextColumns(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "first", Type: catalog.TypeText},
			{Name: "last", Type: catalog.TypeText},
			{Name: "email", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{"John", "Doe", "john@example.com"}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded[0].(string) != "John" {
		t.Errorf("Expected 'John', got %v", decoded[0])
	}

	if decoded[1].(string) != "Doe" {
		t.Errorf("Expected 'Doe', got %v", decoded[1])
	}

	if decoded[2].(string) != "john@example.com" {
		t.Errorf("Expected 'john@example.com', got %v", decoded[2])
	}
}

func TestDecodeRow_TruncatedData(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	// Create truncated data (only first int, missing text)
	truncated := make([]byte, 8) // Only 8 bytes for int, missing text

	_, err := DecodeRow(schema, truncated)
	if err == nil {
		t.Error("Expected error for truncated data")
	}
}

func TestDecodeRow_InvalidTextLength(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "name", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	// Create invalid data with length > available bytes
	invalid := []byte{0, 0, 0, 100} // Length = 100, but only 4 bytes available

	_, err := DecodeRow(schema, invalid)
	if err == nil {
		t.Error("Expected error for invalid text length")
	}
}

func TestDecodeRow_UnsupportedType(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.ColumnType(999)}, // Invalid type
		},
		Indexes: make(map[string]*catalog.Index),
	}

	// Create some data
	data := make([]byte, 8)

	_, err := DecodeRow(schema, data)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}

func TestDecodeRow_AllIntColumns(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "a", Type: catalog.TypeInt},
			{Name: "b", Type: catalog.TypeInt},
			{Name: "c", Type: catalog.TypeInt},
			{Name: "d", Type: catalog.TypeInt},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{1, 2, 3, 4}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	for i, expected := range values {
		if decoded[i].(int) != expected {
			t.Errorf("Value %d: Expected %v, got %v", i, expected, decoded[i])
		}
	}
}

func TestDecodeRow_AllTextColumns(t *testing.T) {
	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "a", Type: catalog.TypeText},
			{Name: "b", Type: catalog.TypeText},
			{Name: "c", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	values := []any{"one", "two", "three"}
	encoded, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := DecodeRow(schema, encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	for i, expected := range values {
		if decoded[i].(string) != expected {
			t.Errorf("Value %d: Expected %v, got %v", i, expected, decoded[i])
		}
	}
}

