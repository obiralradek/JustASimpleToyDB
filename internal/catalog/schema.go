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
	Columns []Column // TODO: change to map for cleaner lookup
	Indexes map[string]*Index
}

type Index struct {
	Name       string
	ColumnName string
}
