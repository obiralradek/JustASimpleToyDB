package catalog

type ColumnType int

const (
	TypeInt ColumnType = iota
	TypeText
)

type Column struct {
	Name string
	Type ColumnType
}

type TableSchema struct {
	Name    string
	Columns []Column
}

func (s *TableSchema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}
