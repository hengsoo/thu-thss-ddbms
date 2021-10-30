package models

import (
	"../labgob"
	"../labrpc"
	"encoding/json"
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
	TableRulesMap map[string][]Rule
	schema        TableSchema
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

	tableRulesMap := make(map[string][]Rule)

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
	c := &Cluster{nodeIds: nodeIds, network: network, Name: clusterName, TableRulesMap: tableRulesMap}
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

// Join all tables in the given list using NATURAL JOIN (join on the common columns), and return the joined result
// as a list of rows and set it to reply.
func (c *Cluster) Join(tableNames []string, reply *Dataset) {
	//TODO lab2
}

// return datatype of a column in a schema given column name
func getColTypeByName(schema TableSchema, colName string) int {
	var colSchemas = schema.ColumnSchemas
	for _, col := range colSchemas {
		if col.Name == colName {
			return col.DataType
		}
	}
	// default to TypeInt32
	return int(0)
}

func (c *Cluster) BuildTable(params []interface{}, reply *string) {
	//schema := params[0]
	//rules := params[1]

	schema := params[0].(TableSchema)
	c.schema = schema
	// Check if the table already exists
	if _, ok := c.TableRulesMap[schema.TableName]; ok {
		reply = nil
		fmt.Sprintf("Table %s already exists in %s cluster", schema.TableName, c.Name)
	} else {
		// Parse rules from unstructured json to map
		var rulesMap map[int]Rule
		json.Unmarshal(params[1].([]byte), &rulesMap)

		// Since there are multiple rules, Slice would be a more intuitive structure for it
		// Convert map_rules from Map to Slice
		for _, value := range rulesMap {
			c.TableRulesMap[schema.TableName] = append(c.TableRulesMap[schema.TableName], value)
		}

		// Example usage of rules
		// fmt.Println("Rules")
		// fmt.Println(c.TableRulesMap[schema.TableName][0].Predicate["BUDGET"][0].Op)
		// fmt.Println(c.TableRulesMap[schema.TableName][0].Predicate["BUDGET"][0].Val)
		// fmt.Println(c.TableRulesMap[schema.TableName][0].Column)

		endNamePrefix := "InternalClient"
		for i := range c.TableRulesMap[schema.TableName] {
			nodeId := c.nodeIds[i]
			endName := endNamePrefix + nodeId
			end := c.network.MakeEnd(endName)
			// connect the client to the node
			c.network.Connect(endName, nodeId)
			// a client should be enabled before being used
			c.network.Enable(endName, true)

			var colSchemas = make([]ColumnSchema, len(c.TableRulesMap[schema.TableName][i].Column))
			var colRules = c.TableRulesMap[schema.TableName][i].Column

			// create column schemas from rules
			for j, colName := range colRules {
				colSchemas[j] = ColumnSchema{Name: colName, DataType: getColTypeByName(schema, colName)}
			}

			// create table schema with name specific to node they live on
			argument := TableSchema{TableName: "PROJ" + strconv.Itoa(i), ColumnSchemas: colSchemas}
			reply := ""

			// ampersand (&) to pass as reference. Needed by Node.CreateTable
			end.Call("Node.BuildTable", &argument, &reply)
			fmt.Println(reply)
		}
	}

}

func isSatisfiedCondition(conditions []Predicate, val interface{}) bool {
	var isSatisfied = true
	switch val.(type) {
	case int:
		for _, cond := range conditions {
			i := float64(val.(int)) - cond.Val.(float64)
			if !(i > 0 && cond.Op == ">") && !(i >= 0 && cond.Op == ">=") && !(i < 0 && cond.Op == "<") && !(i <= 0 && cond.Op == "<=") && !(i == 0 && cond.Op == "=") && !(i != 0 && cond.Op == "!=") {
				isSatisfied = false
			}
		}
		println(val.(int))
	case string:
		for _, cond := range conditions {
			if !(val.(string) == cond.Val.(string) && cond.Op == "=") && !(val.(string) != cond.Val.(string) && cond.Op == "!=") {
				isSatisfied = false
			}
		}
		println(val.(string))
	case float64:
		for _, cond := range conditions {
			i := val.(float64) - cond.Val.(float64)
			if !(i > 0 && cond.Op == ">") && !(i >= 0 && cond.Op == ">=") && !(i < 0 && cond.Op == "<") && !(i <= 0 && cond.Op == "<=") && !(i == 0 && cond.Op == "=") && !(i != 0 && cond.Op == "!=") {
				isSatisfied = false
			}
		}
		println(val.(float64))
	}
	return isSatisfied
}

func (c *Cluster) FragmentWrite(params []interface{}, reply *string) {
	//tableName := params[0]
	//row := params[1]

	// TODO
	// for i, rule in c.TableRulesMap[tableName]
	//		if rule satisfied:
	//			write into node[i]
	fmt.Println("i'm FragmentWrite")
	tableName := params[0]
	//println(params[1].(Row))
	rows := params[1].(Row)

	for _, row := range rows {
		switch row.(type) {
		case int:
			println(row.(int))
		case string:
			println(row.(string))
		case float64:
			println(row.(float64))
		}
	}

	for nodeId, rules := range c.TableRulesMap[tableName.(string)] {
		fmt.Println(nodeId)
		fmt.Println(rules)
		//一条rules有多个Predicate(map string[]Predicate),需要同时满足
		//一个predicate有一个k，即col_name，和多个condition（op，val）
		for k, conditions := range rules.Predicate {
			for index, col := range c.schema.ColumnSchemas {
				if k == col.Name {
					//rows[index]待判断的值
					println(col.Name)
					if isSatisfiedCondition(conditions, rows[index]) {
						println("satisfied")
					} else {
						println("not satisfied")
					}
					//println(index)
					//println(col.DataType)
				}

			}
			//fmt.Println(k)//col name
			//fmt.Println(conditions[0].Op)
			//fmt.Println(conditions[0].Val)
		}
		fmt.Println(rules.Column)
	}
}
