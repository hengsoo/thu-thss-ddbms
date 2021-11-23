package models

type Dataset struct {
	Schema TableSchema
	Rows   []Row
}

// ReconstructTable Reconstruct with dataset and the complete table schema and save to pk-row map
// if allowDuplicate is true, a complete row in node dataset is directly appended to result dataset
func (dataset *Dataset) ReconstructTable(
	pkRowMap map[interface{}]Row,
	fullTableSchema TableSchema,
	allowDuplicate bool,
	result *Dataset) {

	// the respective indices of fragmented column schemas in the complete list of column schemas
	var insertColIdxs []int

	for _, colSchema := range dataset.Schema.ColumnSchemas {
		insertColName := colSchema.Name
		insertColIdxs = append(insertColIdxs, fullTableSchema.GetColIndexByName(insertColName))
	}

	fullTableColumnsLen := len(fullTableSchema.ColumnSchemas)

	// Insert/Merge rows
	for _, nodeRow := range dataset.Rows {

		var primaryKey interface{} = nodeRow[0]

		// If the nodeRow schema is complete, just insert it to the result
		if len(nodeRow) == fullTableColumnsLen {
			// directly append to result if allows duplicate, pkRowMap results will be appended to result.Rows in caller
			if allowDuplicate {
				result.Rows = append(result.Rows, nodeRow)
			} else {
				pkRowMap[primaryKey] = nodeRow
			}

		} else {
			// If PK doesn't exist, create new Row
			if _, ok := pkRowMap[primaryKey]; !ok {
				pkRowMap[primaryKey] = make(Row, fullTableColumnsLen)
			}
			// Insert data into rows
			for nodeColIdx, insertColIdx := range insertColIdxs {
				pkRowMap[primaryKey][insertColIdx] = nodeRow[nodeColIdx]
			}
		}
	}
}
