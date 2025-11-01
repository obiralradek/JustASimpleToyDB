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
