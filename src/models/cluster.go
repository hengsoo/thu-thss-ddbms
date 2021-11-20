package models

import (
	"../labgob"
	"../labrpc"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Cluster consists of a group of nodes to manage distributed tables defined in models/table.go.
// The Cluster object itself can also be viewed as the only coordinator of a cluster, which means client requests
// should go through it instead of the nodes.
// Of course, it is possible to make any of the nodes a coordinator and to make the cluster decentralized. You are
// welcomed to make such changes and may earn some extra points.
type Cluster struct {
	// the identifiers of each node, we use simple numbers like "1,2,3" to register the nodes in the network
	// needless to say, each identifier should be unique
	nodeIds []string
	// the network that the cluster works on. It is not actually using the network interface, but a network simulator
	// using SEDA (google it if you have not heard about it), which allows us (and you) to inject some network failures
	// during tests. Do remember that network failures should always be concerned in a distributed environment.
	network *labrpc.Network
	// the Name of the cluster, also used as a network address of the cluster coordinator in the network above
	Name string

	// Segmentation rules for tables
	// TableRulesMap[tableName][nodeIdxStr] -> Rule for node[nodeIdxStr]
	TableRulesMap map[string]map[string]Rule
	// TableSchemasMap[tableName] -> TableSchema
	TableSchemasMap map[string]TableSchema
}

// NewCluster creates a Cluster with the given number of nodes and register the nodes to the given network.
// The created cluster will be named with the given one, which will used when a client wants to connect to the cluster
// and send requests to it. WARNING: the given name should not be like "Node0", "Node1", ..., as they will conflict
// with some predefined names.
// The created nodes are identified by simple numbers starting from 0, e.g., if we have 3 nodes, the identifiers of the
// three nodes will be "Node0", "Node1", and "Node2".
// Each node is bound to a server in the network which follows the same naming rule, for the example above, the three
// nodes will be bound to  servers "Node0", "Node1", and "Node2" respectively.
// In practice, we may mix the usages of terms "Node" and "Server", both of them refer to a specific machine, while in
// the lab, a "Node" is responsible for processing distributed affairs but a "Server" simply receives messages from the
// net work.
func NewCluster(nodeNum int, network *labrpc.Network, clusterName string) *Cluster {
	labgob.Register(TableSchema{})
	labgob.Register(Row{})

	tableRulesMap := make(map[string]map[string]Rule)
	tableSchemasMap := make(map[string]TableSchema)

	nodeIds := make([]string, nodeNum)
	nodeNamePrefix := "Node"
	for i := 0; i < nodeNum; i++ {
		// identify the nodes with "Node0", "Node1", ...
		node := NewNode(nodeNamePrefix + strconv.Itoa(i))
		nodeIds[i] = node.Identifier
		// use go reflection to extract the methods in a Node object and make them as a service.
		// a service can be viewed as a list of methods that a server provides.
		// due to the limitation of the framework, the extracted method must only have two parameters, and the first one
		// is the actual argument list, while the second one is the reference to the result.
		// NOTICE, a REFERENCE should be passed to the method instead of a value
		nodeService := labrpc.MakeService(node)
		// create a server, a server is responsible for receiving requests and dispatching them
		server := labrpc.MakeServer()
		// add the service to the server so the server can provide the services
		server.AddService(nodeService)
		// register the server to the network as "Node0", "Node1", ...
		network.AddServer(nodeIds[i], server)
	}

	// create a cluster with the nodes and the network
	c := &Cluster{nodeIds: nodeIds, network: network, Name: clusterName,
		TableRulesMap: tableRulesMap, TableSchemasMap: tableSchemasMap}
	// create a coordinator for the cluster to receive external requests, the steps are similar to those above.
	// notice that we use the reference of the cluster as the name of the coordinator server,
	// and the names can be more than strings.
	clusterService := labrpc.MakeService(c)
	server := labrpc.MakeServer()
	server.AddService(clusterService)
	network.AddServer(clusterName, server)
	return c
}

// SayHello is an example to show how the coordinator communicates with other nodes in the cluster.
// Any method that can be accessed by network clients should have EXACTLY TWO parameters, while the first one is the
// actual parameter desired by the method (can be a list if there are more than one desired parameters), and the second
// one is a reference to the return value. The caller must ensure that the reference is valid (not nil).
func (c *Cluster) SayHello(visitor string, reply *string) {
	endNamePrefix := "InternalClient"
	for _, nodeId := range c.nodeIds {
		// create a client (end) to each node
		// the name of the client should be unique, so we use the name of each node for it
		endName := endNamePrefix + nodeId
		end := c.network.MakeEnd(endName)
		// connect the client to the node
		c.network.Connect(endName, nodeId)
		// a client should be enabled before being used
		c.network.Enable(endName, true)
		// call method on that node
		argument := visitor
		reply := ""
		// the first parameter is the name of the method to be called, recall that we use the reference of
		// a Node object to create a service, so the first part of the parameter will be the class name "Node", and as
		// we want to call the method SayHello(), so the second part is "SayHello", and the two parts are separated by
		// a dot
		end.Call("Node.SayHello", argument, &reply)
		fmt.Println(reply)
	}
	*reply = fmt.Sprintf("Hello %s, I am the coordinator of %s", visitor, c.Name)
}

// GetFullTableDataset by joining all the tables with the same name in all relevant nodes.
// The return Dataset will have a complete tableSchema as stored in the cluster.
// The join is based on primary key of each table. The first column in each nodes' tableSchema is assumed to be the PK.
func (c *Cluster) GetFullTableDataset(tableName string, result *Dataset) error {
	// Get table schema
	// Check if the table already exists
	if _, ok := c.TableSchemasMap[tableName]; ok {
		*result = Dataset{}
		result.Schema = c.TableSchemasMap[tableName]

		// Map of primary key to its row
		// The first column in each node' tableSchema is assumed to be the PK.
		pkRowMap := make(map[interface{}]Row)
		fullTableColumnsLen := len(result.Schema.ColumnSchemas)

		// Iterate node by relevant rule
		// Get partial row data from each node
		endNamePrefix := "InternalClient"
		for nodeIdxStr, rule := range c.TableRulesMap[tableName] {

			nodeIdx, _ := strconv.Atoi(nodeIdxStr)
			nodeId := c.nodeIds[nodeIdx]
			endName := endNamePrefix + nodeId
			end := c.network.MakeEnd(endName)
			// connect the client to the node
			c.network.Connect(endName, nodeId)
			// a client should be enabled before being used
			c.network.Enable(endName, true)

			var insertColIdxs []int
			for _, insertColName := range rule.Column {
				insertColIdxs = append(insertColIdxs, result.Schema.GetColIndexByName(insertColName))
			}

			var nodeTableDataset Dataset
			end.Call("Node.GetTableDataset", tableName, &nodeTableDataset)

			// Insert/Merge rows
			for _, nodeRow := range nodeTableDataset.Rows {

				// If the nodeRow schema is complete, just insert it to the result
				// !! Duplication is not handled here !!
				if len(nodeRow) == fullTableColumnsLen {
					result.Rows = append(result.Rows, nodeRow)
				} else {
					// Else nodeRow schema is partial, merge it to pkRowMap
					var primaryKey interface{} = nodeRow[0]
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

		// Add rows to result
		for _, row := range pkRowMap {
			result.Rows = append(result.Rows, row)
		}

		return nil
	} else {
		// If table doesn't exist
		return errors.New("table " + tableName + " doesn't exist")
	}

}

// NaturalJoinDatasets by matching all common columns.
// Datasets are passed as references to avoid expensive copying.
func (c *Cluster) NaturalJoinDatasets(datasetPtrs []*Dataset) (Dataset, error) {

	datasetPtrsLen := len(datasetPtrs)

	if datasetPtrsLen < 2 {
		return Dataset{}, errors.New("number of datasetPtrs should be more than 2")
	}

	result := Dataset{}

	// Joined tableName should be empty
	result.Schema.TableName = ""
	result.Schema = datasetPtrs[0].Schema
	result.Rows = datasetPtrs[0].Rows

	for datasetPtrIdx := 1; datasetPtrIdx < datasetPtrsLen; datasetPtrIdx++ {

		dataset := *datasetPtrs[datasetPtrIdx]

		// Map dataset -> result common column indexes
		commonColsIdxMap := make(map[int]int)
		var tempColSchemas []ColumnSchema
		for datasetColIdx, datasetColSchema := range dataset.Schema.ColumnSchemas {
			resultColIdx := result.Schema.GetColIndexByName(datasetColSchema.Name)
			// If there is a common column, add it to commonColsIdx
			if resultColIdx != -1 {
				commonColsIdxMap[datasetColIdx] = resultColIdx
			} else {
				//	Else add the schema into tempColSchemas
				tempColSchemas = append(tempColSchemas, datasetColSchema)
			}
		}

		// If there are no common columns, clear result and short-circuit
		if len(commonColsIdxMap) == 0 {
			result = Dataset{}
			fmt.Println("Natural Join(s) has(have) no common columns.")
			return result, nil
		}

		// Add tempColSchemas to result
		beforeJoinResultRowLen := len(result.Rows)
		beforeJoinResultColLen := len(result.Schema.ColumnSchemas)
		result.Schema.ColumnSchemas = append(result.Schema.ColumnSchemas, tempColSchemas...)

		// Join dataset and result
		hasJoinResult := false
		for _, datasetRow := range dataset.Rows {
			for resultRowIdx := 0; resultRowIdx < beforeJoinResultRowLen; resultRowIdx++ {

				resultRow := result.Rows[resultRowIdx]

				// Check conditions
				matched := true
				for datasetColIdx, resultColIdx := range commonColsIdxMap {
					if resultRow[resultColIdx] != datasetRow[datasetColIdx] {
						matched = false
						break
					}
				}

				if matched {
					hasJoinResult = true
					var appendRowPtr *Row = &result.Rows[resultRowIdx]

					// If there are duplicate matches, copy and insert data
					if len(*appendRowPtr) > beforeJoinResultColLen {
						newRow := make(Row, beforeJoinResultColLen)
						// Note: copy(dst, src) copies min(len(dst), len(src)) elements.
						copy(newRow, result.Rows[resultRowIdx])
						result.Rows = append(result.Rows, newRow)
						// Set the appended row
						appendRowPtr = &result.Rows[len(result.Rows)-1]
					}

					// Append non-common columns data
					for datasetColIdx, datasetColVal := range datasetRow {
						if _, ok := commonColsIdxMap[datasetColIdx]; !ok {
							*appendRowPtr = append(*appendRowPtr, datasetColVal)
						}
					}
				}
			}
		}

		// If a dataset has no join result, clear result's rows and short-circuit
		if !hasJoinResult {
			result.Rows = nil
			fmt.Println("Natural Join(s) has(have) no matching results.")
			return result, nil
		}
	}

	return result, nil
}

// Join all tables in the given list using NATURAL JOIN (join on the common columns)
// Set reply as a Dataset of the joined results.
func (c *Cluster) Join(tableNames []string, reply *Dataset) {
	// GetFullTableDataset of tableNames
	datasetPtrs := make([]*Dataset, len(tableNames))
	var err error
	for i, tableName := range tableNames {
		datasetPtrs[i] = &Dataset{}
		err = c.GetFullTableDataset(tableName, datasetPtrs[i])
		if err != nil {
			reply = nil
			fmt.Println(err.Error())
			return
		}
	}

	// Then join them using NaturalJoinDataset
	if result, err := c.NaturalJoinDatasets(datasetPtrs); err != nil {
		reply = nil
		fmt.Println(err.Error())
	} else {
		*reply = result
	}
}

// Semi Join first TWO* tables in the given list using provided column name
// Set reply as a Dataset of the joined results.
func (c *Cluster) SemiJoin(params []string, reply *Dataset) {

	// the column name to join the two tables on
	var onJoinColName = params[0]

	datasetPtrs := make([]*Dataset, len(params) - 1)

	var err error

	// get full dataset for each table name
	for i, tableName := range params {
		// first item should be column name to join on, skip
		if i == 0 {
			continue
		}
		// initialize dataset pointer
		datasetPtrs[i-1] = &Dataset{}

		// get full dataset and store it into list of pointer, -1 for the offset of provided column name
		err = c.GetFullTableDataset(tableName, datasetPtrs[i-1])
		if err != nil {
			reply = nil
			fmt.Println(err.Error())
			return
		}
	}
	dataset1 := datasetPtrs[0]
	dataset2 := datasetPtrs[1]

	// short circuit and return if both tables doesn't have the column to join on
	if dataset1.Schema.GetColIndexByName(onJoinColName) == -1 || dataset2.Schema.GetColIndexByName(onJoinColName) == -1 {
		reply = nil
		fmt.Println("Column to join doesn't exist in both table")
		return
	}

	// index of column to be joined on, in table 2, call it source table
	srcColIndex := int(dataset2.Schema.GetColIndexByName(onJoinColName))

	// a hashmap storing the possible values in the on-join column in table 2
	rowItemExistsMap := make(map[interface{}]bool)

	// set the value to true indicating it exists
	for _, row := range dataset2.Rows {
		rowItemExistsMap[row[srcColIndex]] = true
	}

	*reply = Dataset{}
	reply.Schema = dataset1.Schema

	// index of column to be joined on, in table 1, the table with the schema to be returned
	tgtColIndex := int(dataset1.Schema.GetColIndexByName(onJoinColName))

	// only add the rows that have existing counterpart in on join column in table 2
	for _, row := range dataset1.Rows {
		if rowItemExistsMap[row[tgtColIndex]] {
			reply.Rows = append(reply.Rows, row)
		}
	}
}

func (c *Cluster) BuildTable(params []interface{}, reply *string) {
	//schema := params[0]
	//rules := params[1]

	schema := params[0].(TableSchema)
	c.TableSchemasMap[schema.TableName] = schema

	// Check if the table already exists
	if _, ok := c.TableRulesMap[schema.TableName]; ok {
		reply = nil
		_ = fmt.Sprintf("Table %s already exists in %s cluster", schema.TableName, c.Name)
	} else {
		// Parse rules from unstructured json to map
		var rulesMap map[string]Rule
		_ = json.Unmarshal(params[1].([]byte), &rulesMap)
		c.TableRulesMap[schema.TableName] = rulesMap

		// Example usage of rules
		// fmt.Println("Rules")
		// fmt.Println(c.TableRulesMap[schema.TableName]["0"].Predicate["BUDGET"][0].Op)
		// fmt.Println(c.TableRulesMap[schema.TableName]["0"].Predicate["BUDGET"][0].Val)
		// fmt.Println(c.TableRulesMap[schema.TableName]["0"].Column)

		endNamePrefix := "InternalClient"
		// Foreach rule of table
		// TableRulesMap[tableName][nodeIdxStr] -> Rule for node[nodeIdxStr]
		for nodeIdxStr, rule := range c.TableRulesMap[schema.TableName] {
			nodeIdx, _ := strconv.Atoi(nodeIdxStr)
			nodeId := c.nodeIds[nodeIdx]
			endName := endNamePrefix + nodeId
			end := c.network.MakeEnd(endName)
			// connect the client to the node
			c.network.Connect(endName, nodeId)
			// a client should be enabled before being used
			c.network.Enable(endName, true)

			var colSchemas = make([]ColumnSchema, len(rule.Column))

			// create column schemas from rules
			for colIdx, colName := range rule.Column {
				colSchemas[colIdx] = ColumnSchema{Name: colName, DataType: schema.GetColTypeByName(colName)}
			}

			// create table schema with name specific to node they live on
			argument := TableSchema{TableName: schema.TableName, ColumnSchemas: colSchemas}
			reply := ""

			// ampersand (&) to pass as reference. Needed by Node.CreateTable
			end.Call("Node.BuildTable", &argument, &reply)
			//fmt.Println(reply)
		}
	}

}

func (c *Cluster) FragmentWrite(params []interface{}, reply *string) {
	//tableName := params[0]
	//row := params[1]

	tableName := params[0].(string)
	// Un-partitioned row (follows cluster's table schema)
	row := params[1].(Row)
	schema := c.TableSchemasMap[tableName]

	endNamePrefix := "InternalClient"
	// Foreach rule of table
	// TableRulesMap[tableName][nodeIdxStr] -> Rule for node[nodeIdxStr]
	for nodeIdxStr, rule := range c.TableRulesMap[tableName] {

		isAllPredicatesSatisfied := true

		for colName, colConditions := range rule.Predicate {
			if !row.SatisfiesColumnConditions(schema, colName, colConditions) {
				isAllPredicatesSatisfied = false
				break
			}
		}

		if isAllPredicatesSatisfied == false {
			continue
		}

		nodeIdx, _ := strconv.Atoi(nodeIdxStr)
		nodeId := c.nodeIds[nodeIdx]
		endName := endNamePrefix + nodeId
		end := c.network.MakeEnd(endName)
		// connect the client to the node
		c.network.Connect(endName, nodeId)
		// a client should be enabled before being used
		c.network.Enable(endName, true)

		var newRow Row
		for _, colName := range rule.Column {
			newRow = append(newRow, row[schema.GetColIndexByName(colName)])
		}

		end.Call("Node.FragmentWrite", []interface{}{tableName, newRow}, &reply)
		//fmt.Println(reply)
	}
}
