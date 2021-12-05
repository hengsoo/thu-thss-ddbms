package models

type Dataset struct {
	Schema TableSchema
	Rows   []Row
}

// ReconstructTable reconstruct dataset with fullTableSchema and save to _pkRowMap
func (dataset *Dataset) ReconstructTable(
	_pkRowMap map[interface{}]Row,
	fullTableSchema TableSchema) {

	// the respective indices of fragmented column schemas in the complete list of column schemas
	var insertColIdxs []int

	for _, colSchema := range dataset.Schema.ColumnSchemas {
		insertColName := colSchema.Name
		insertColIdxs = append(insertColIdxs, fullTableSchema.GetColIndexByName(insertColName))
	}

	fullTableColumnsLen := len(fullTableSchema.ColumnSchemas)

	// Insert/Merge rows
	for _, nodeRow := range dataset.Rows {

		// Note: We assume the pk to be the node row idx of un-partitioned table
		var primaryKey interface{} = nodeRow[0]

		// If PK doesn't exist, create new Row
		if _, ok := _pkRowMap[primaryKey]; !ok {
			_pkRowMap[primaryKey] = make(Row, fullTableColumnsLen)
		}
		// Insert data into rows
		for nodeColIdx, insertColIdx := range insertColIdxs {
			// Note: Add one to skip the row idx in row ( row idx is hidden from schema )
			_pkRowMap[primaryKey][insertColIdx] = nodeRow[nodeColIdx+1]
		}
	}
}
