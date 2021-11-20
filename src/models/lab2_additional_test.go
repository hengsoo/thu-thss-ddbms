package models

import (
	"../labrpc"
	"encoding/json"
	"testing"
	"fmt"
)

// Multiple Tables Join I
func TestLab2Additional3TablesJoin(t *testing.T) {
	// TODO
	t.Fatal("Test not implemented")
}

// Multiple Tables Join II
func TestLab2Additional5TablesJoin(t *testing.T) {
	// TODO
	t.Fatal("Test not implemented")
}

// TODO
// Please add more relevant tests on Multiple Tables Join


// Semi Join I
func defineSemiJoinTables() {
	studentTableSchema = &TableSchema{TableName: studentTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "grade", DataType: TypeFloat},
	}}

	courseRegistrationTableSchema = &TableSchema{TableName: courseRegistrationTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "courseId", DataType: TypeInt32},
	}}

	studentRows = []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
		{3, "Eve", 21, 3.2},
		{4, "Lewis", 21, 3.0},
	}

	courseRegistrationRows = []Row{
		{0, 0},
		{2, 1},
		{1, 0},
		{4, 0},
	}
}

func semiJoinSetup() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network = labrpc.MakeNetwork()
	c = NewCluster(3, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli = network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	defineSemiJoinTables()
}

func TestLab2NonOverlappingSemiJoin(t *testing.T) {
	semiJoinSetup()
	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0": map[string]interface{}{
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
		"1": map[string]interface{}{
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
		"2": map[string]interface{}{
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

	buildTables(cli)
	insertData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.SemiJoin", []string{"sid",studentTableName, courseRegistrationTableName}, &results)

	joinedTableContent = []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
		{4, "Lewis", 21, 3.0},
	}

	expectedDataset := Dataset{
		Schema: *studentTableSchema,
		Rows:   joinedTableContent,
	}

	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect semi join results, expected %v, actual %v", expectedDataset, results)
	}
	fmt.Println("--------------------")
	fmt.Println("SemiJoin Non overlap test passed")
	fmt.Println("--------------------")
}