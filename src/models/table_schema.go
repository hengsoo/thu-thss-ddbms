package models

// TableSchema contains the name of the table and the definition of each column
type TableSchema struct {
	TableName     string
	ColumnSchemas []ColumnSchema
}

// Get (column idx, column data type) by name
// Return -1,-1 if none is found
func (schema *TableSchema) GetColumnByName(colName string) (int, int) {
	var colSchemas = schema.ColumnSchemas
	for idx, col := range colSchemas {
		if col.Name == colName {
			return idx, col.DataType
		}
	}
	// default
	return -1, -1
}

// Get column idx by name
// Return -1 if none is found
func (schema *TableSchema) GetColIndexByName(colName string) int {
	colIdx, _ := schema.GetColumnByName(colName)
	return colIdx
}

// Get column data type by name
// Return -1 if none is found
func (schema *TableSchema) GetColTypeByName(colName string) int {
	_, colType := schema.GetColumnByName(colName)
	return colType
}
