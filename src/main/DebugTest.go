package main

import (
	"../labrpc"
	"../models"
	"encoding/json"
)

const studentTableName = "student"
const courseRegistrationTableName = "courseRegistration"

var c *models.Cluster
var network *labrpc.Network
var cli *labrpc.ClientEnd

var studentTableSchema *models.TableSchema
var courseRegistrationTableSchema *models.TableSchema

var studentRows []models.Row
var courseRegistrationRows []models.Row

var joinedTableSchema models.TableSchema
var joinedTableContent []models.Row

var studentTablePartitionRules []byte
var courseRegistrationTablePartitionRules []byte

// This file is used to debug tests.
// Copy and paste test's content here and run as debug mode to debug.
// Note: this is done so due to lack of debugging support in running tests (Intellij IDE).
func main() {

	setup()

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
	results := models.Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	//expectedDataset := models.Dataset{
	//	Schema: joinedTableSchema,
	//	Rows: joinedTableContent,
	//}
	//if !models.compareDataset(expectedDataset, results) {
	//	_ = fmt.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	//}
	return
}

func defineTables() {
	studentTableSchema = &models.TableSchema{TableName: studentTableName, ColumnSchemas: []models.ColumnSchema{
		{Name: "sid", DataType: models.TypeInt32},
		{Name: "name", DataType: models.TypeString},
		{Name: "age", DataType: models.TypeInt32},
		{Name: "grade", DataType: models.TypeFloat},
	}}

	courseRegistrationTableSchema = &models.TableSchema{TableName: courseRegistrationTableName, ColumnSchemas: []models.ColumnSchema{
		{Name: "sid", DataType: models.TypeInt32},
		{Name: "courseId", DataType: models.TypeInt32},
	}}

	studentRows = []models.Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
	}

	courseRegistrationRows = []models.Row{
		{0, 0},
		{0, 1},
		{1, 0},
		{2, 2},
	}

	joinedTableSchema = models.TableSchema{
		"",
		[]models.ColumnSchema{
			{"sid", models.TypeInt32},
			{"name", models.TypeString},
			{"age", models.TypeInt32},
			{"grade", models.TypeFloat},
			{"courseId", models.TypeInt32},
		},
	}

	joinedTableContent = []models.Row{
		{0, "John", 22, 4.0, 0},
		{0, "John", 22, 4.0, 1},
		{1, "Smith", 23, 3.6, 0},
		{2, "Hana", 21, 4.0, 2},
	}
}

func setup() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network = labrpc.MakeNetwork()
	c = models.NewCluster(3, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli = network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	defineTables()
}

func buildTables(cli *labrpc.ClientEnd) {
	replyMsg := ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{courseRegistrationTableSchema, courseRegistrationTablePartitionRules}, &replyMsg)
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{studentTableSchema, studentTablePartitionRules}, &replyMsg)
}

func insertData(cli *labrpc.ClientEnd) {
	replyMsg := ""
	for _, row := range studentRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentTableName, row}, &replyMsg)
	}

	replyMsg = ""
	for _, row := range courseRegistrationRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseRegistrationTableName, row}, &replyMsg)
	}
}
