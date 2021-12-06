package models

import (
	"encoding/json"
	"testing"
)

// student table is held by three nodes and courseRegistration table is held by the last node
func TestLab3AdditionalVerticalFragmentation(t *testing.T) {
	setupLab3()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node2
	m = map[string]interface{}{
		"3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	buildTablesLab3(cli)
	insertDataLab3(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema,
		Rows:   joinedTableContent,
	}
	if !datasetDuplicateChecking(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}