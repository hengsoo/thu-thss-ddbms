package models

import (
	"../labrpc"
	"encoding/json"
	"testing"
)

const teacherTableName = "teacher"
const studentClassTableName = "studentClass"
const teacherSchoolTableName = "teacherSchool"

var teacherTableSchema *TableSchema
var studentClassTableSchema *TableSchema
var teacherSchoolTableSchema *TableSchema

var teacherRows []Row
var studentClassRows []Row
var teacherSchoolRows []Row

var joined3TableSchema TableSchema
var joined3TableContent []Row
var joined5TableSchema TableSchema
var joined5TableContent []Row

var teacherTablePartitionRules []byte
var studentClassTablePartitionRules []byte
var teacherSchoolTablePartitionRules []byte

func defineMultiTables() {
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

	teacherTableSchema = &TableSchema{TableName: teacherTableName, ColumnSchemas: []ColumnSchema{
		{Name: "tid", DataType: TypeInt32},
		{Name: "courseId", DataType: TypeInt32},
	}}

	studentClassTableSchema = &TableSchema{TableName: studentClassTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "class", DataType: TypeString},
	}}

	teacherSchoolTableSchema = &TableSchema{TableName: teacherSchoolTableName, ColumnSchemas: []ColumnSchema{
		{Name: "tid", DataType: TypeInt32},
		{Name: "school", DataType: TypeString},
	}}

	studentRows = []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
	}

	courseRegistrationRows = []Row{
		{0, 0},
		{0, 1},
		{1, 0},
		{2, 2},
	}

	teacherRows = []Row{
		{0, 1},
		{1, 2},
		{2, 0},
	}

	studentClassRows = []Row{
		{0, "John", 22, "81"},
		{1, "Smith", 23, "82"},
		{2, "Hana", 21, "83"},
	}

	teacherSchoolRows = []Row{
		{0, "SS"},
		{1, "SEM"},
		{2, "CS"},
	}

	joined3TableSchema = TableSchema{
		"",
		[]ColumnSchema{
			{"sid", TypeInt32},
			{"name", TypeString},
			{"age", TypeInt32},
			{"grade", TypeFloat},
			{"courseId", TypeInt32},
			{"tid", TypeInt32},
		},
	}

	joined3TableContent = []Row{
		{0, "John", 22, 4.0, 0, 2},
		{0, "John", 22, 4.0, 1, 0},
		{1, "Smith", 23, 3.6, 0, 2},
		{2, "Hana", 21, 4.0, 2, 1},
	}

	joined5TableSchema = TableSchema{
		"",
		[]ColumnSchema{
			{"sid", TypeInt32},
			{"name", TypeString},
			{"age", TypeInt32},
			{"grade", TypeFloat},
			{"courseId", TypeInt32},
			{"tid", TypeInt32},
			{"class", TypeString},
			{"school", TypeString},
		},
	}

	joined5TableContent = []Row{
		{0, "John", 22, 4.0, 0, 2, "81", "CS"},
		{0, "John", 22, 4.0, 1, 0, "81", "SS"},
		{1, "Smith", 23, 3.6, 0, 2, "82", "CS"},
		{2, "Hana", 21, 4.0, 2, 1, "83", "SEM"},
	}
}

func MultiTableSetup() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network = labrpc.MakeNetwork()
	c = NewCluster(5, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli = network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	defineMultiTables()
}

// Multiple Tables Join I NonOverlapping every table is held by different node
func TestLab2Additional3TablesJoin(t *testing.T) {
	// TODO
	MultiTableSetup()
	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
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
		"1": map[string]interface{}{
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

	// assign teacher table to node0
	m = map[string]interface{}{
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"tid", "courseId",
			},
		},
	}
	teacherTablePartitionRules, _ = json.Marshal(m)

	buildThreeTables(cli)
	insertThreeData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName, teacherTableName}, &results)
	expectedDataset := Dataset{
		Schema: joined3TableSchema,
		Rows:   joined3TableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// Multiple Tables Join II NonOverlapping every table is held by different node
func TestLab2Additional5TablesJoin(t *testing.T) {
	// TODO
	MultiTableSetup()
	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
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
		"1": map[string]interface{}{
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

	// assign teacher table to node0
	m = map[string]interface{}{
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"tid", "courseId",
			},
		},
	}
	teacherTablePartitionRules, _ = json.Marshal(m)

	// assign studentClass table to node3
	m = map[string]interface{}{
		"3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "class",
			},
		},
	}
	studentClassTablePartitionRules, _ = json.Marshal(m)

	// assign teacherSchool table to node4
	m = map[string]interface{}{
		"4": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"tid", "school",
			},
		},
	}
	teacherSchoolTablePartitionRules, _ = json.Marshal(m)

	buildFiveTables(cli)
	insertFiveData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName,
		teacherTableName, studentClassTableName, teacherSchoolTableName}, &results)
	expectedDataset := Dataset{
		Schema: joined5TableSchema,
		Rows:   joined5TableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// TODO
// Please add more relevant tests on Multiple Tables Join

// Multiple Tables Join I NonOverlapping some table is held by same node
func TestLab2Additional3TablesJoinOverlapping(t *testing.T) {
	// TODO
	MultiTableSetup()
	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  "<",
					"val": 1,
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
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	// assign teacher table to node0,2
	m = map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"tid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  "<",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"tid", "courseId",
			},
		},
	}
	teacherTablePartitionRules, _ = json.Marshal(m)

	buildThreeTables(cli)
	insertThreeData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName, teacherTableName}, &results)
	expectedDataset := Dataset{
		Schema: joined3TableSchema,
		Rows:   joined3TableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// Multiple Tables Join II NonOverlapping some table is held by same node
func TestLab2Additional5TablesJoinOverlapping(t *testing.T) {
	// TODO
	MultiTableSetup()
	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node3
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.7,
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
					"op":  "<=",
					"val": 3.7,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node1,2
	m = map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  "<",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	// assign teacher table to node0,2
	m = map[string]interface{}{
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"tid", "courseId",
			},
		},
		"3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  "<",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"tid", "courseId",
			},
		},
	}
	teacherTablePartitionRules, _ = json.Marshal(m)

	// assign studentClass table to node1,3
	m = map[string]interface{}{
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "class",
			},
		},
	}
	studentClassTablePartitionRules, _ = json.Marshal(m)

	// assign teacherSchool table to node4
	m = map[string]interface{}{
		"3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"tid", "school",
			},
		},
	}
	teacherSchoolTablePartitionRules, _ = json.Marshal(m)

	buildFiveTables(cli)
	insertFiveData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName,
		teacherTableName, studentClassTableName, teacherSchoolTableName}, &results)
	expectedDataset := Dataset{
		Schema: joined5TableSchema,
		Rows:   joined5TableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

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
	cli.Call("Cluster.SemiJoin", []string{"sid", studentTableName, courseRegistrationTableName}, &results)

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
}

func TestLab2FullyOverlappingSemiJoin(t *testing.T) {
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
		"1": map[string]interface{}{
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
	cli.Call("Cluster.SemiJoin", []string{"sid", studentTableName, courseRegistrationTableName}, &results)

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

}

func TestLab2FullyCentralizedSemiJoin(t *testing.T) {
	semiJoinSetup()
	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0.0,
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
		"0": map[string]interface{}{
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
	cli.Call("Cluster.SemiJoin", []string{"sid", studentTableName, courseRegistrationTableName}, &results)

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

}

func TestLab2PartiallyOverlappingSemiJoin(t *testing.T) {
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
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">",
					"val": 1,
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
	cli.Call("Cluster.SemiJoin", []string{"sid", studentTableName, courseRegistrationTableName}, &results)

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

}

// courseRegistration table is empty in this test
func TestLab2EmptyTableSemiJoin(t *testing.T) {
	semiJoinSetup()

	courseRegistrationRows = []Row{}
	joinedTableContent = []Row{}

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

	// assign course registration to node1 and node2
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">",
					"val": 1,
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
	cli.Call("Cluster.SemiJoin", []string{"sid", studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: *studentTableSchema,
		Rows:   joinedTableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// there is no matching tuple in this test
func TestLab2NoMatchingSemiJoin(t *testing.T) {
	semiJoinSetup()

	courseRegistrationRows = []Row{
		{10, 0},
		{10, 1},
		{11, 0},
		{12, 2},
	}
	joinedTableContent = []Row{}

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

	// assign course registration to node1 and node2
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">",
					"val": 1,
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
	cli.Call("Cluster.SemiJoin", []string{"sid", studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: *studentTableSchema,
		Rows:   joinedTableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

func TestLab2MissingPrimaryKeySemiJoin(t *testing.T) {
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
				"sid", "name",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"age", "grade",
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
	cli.Call("Cluster.SemiJoin", []string{"sid", studentTableName, courseRegistrationTableName}, &results)

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
}

func buildThreeTables(cli *labrpc.ClientEnd) {
	//buildTables
	replyMsg := ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{courseRegistrationTableSchema, courseRegistrationTablePartitionRules}, &replyMsg)

	replyMsg = ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{studentTableSchema, studentTablePartitionRules}, &replyMsg)

	replyMsg = ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{teacherTableSchema, teacherTablePartitionRules}, &replyMsg)

}

func insertThreeData(cli *labrpc.ClientEnd) {
	//insertData
	replyMsg := ""
	for _, row := range studentRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentTableName, row}, &replyMsg)
	}
	replyMsg = ""
	for _, row := range courseRegistrationRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseRegistrationTableName, row}, &replyMsg)
	}
	replyMsg = ""
	for _, row := range teacherRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{teacherTableName, row}, &replyMsg)
	}
}

func buildFiveTables(cli *labrpc.ClientEnd) {
	//buildTables
	replyMsg := ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{courseRegistrationTableSchema, courseRegistrationTablePartitionRules}, &replyMsg)

	replyMsg = ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{studentTableSchema, studentTablePartitionRules}, &replyMsg)

	replyMsg = ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{teacherTableSchema, teacherTablePartitionRules}, &replyMsg)

	replyMsg = ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{teacherSchoolTableSchema, teacherSchoolTablePartitionRules}, &replyMsg)

	replyMsg = ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{studentClassTableSchema, studentClassTablePartitionRules}, &replyMsg)

}

func insertFiveData(cli *labrpc.ClientEnd) {
	//insertData
	replyMsg := ""
	for _, row := range studentRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentTableName, row}, &replyMsg)
	}
	replyMsg = ""
	for _, row := range courseRegistrationRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseRegistrationTableName, row}, &replyMsg)
	}
	replyMsg = ""
	for _, row := range teacherRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{teacherTableName, row}, &replyMsg)
	}
	replyMsg = ""
	for _, row := range teacherSchoolRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{teacherSchoolTableName, row}, &replyMsg)
	}
	replyMsg = ""
	for _, row := range studentClassRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentClassTableName, row}, &replyMsg)
	}
}
