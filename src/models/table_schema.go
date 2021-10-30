package models

// TableSchema contains the name of the table and the definition of each column
type TableSchema struct {
	TableName     string
	ColumnSchemas []ColumnSchema
}

// Get (column idx, column data type) by name
func (schema *TableSchema) getColumnSchemaByName(colName string) (int, int) {
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
func (schema *TableSchema) getColIndexByName(colName string) int {
	colIdx, _ := schema.getColumnSchemaByName(colName)
	return colIdx
}

// Get column data type by name
func (schema *TableSchema) getColTypeByName(colName string) int {
	_, colType := schema.getColumnSchemaByName(colName)
	return colType
}
