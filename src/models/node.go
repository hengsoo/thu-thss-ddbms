package models

import (
	"errors"
	"fmt"
	"strconv"
)

// Node manages some tables defined in models/table.go
type Node struct {
	// the name of the Node, and it should be unique across the cluster
	Identifier string
	// tableName -> table
	TableMap map[string]*Table
}
type ValueSet map[interface{}]bool

// NewNode creates a new node with the given name and an empty set of tables
func NewNode(id string) *Node {
	return &Node{TableMap: make(map[string]*Table), Identifier: id}
}

// SayHello is an example about how to create a method that can be accessed by RPC (remote procedure call, methods that
// can be called through network from another node). RPC methods should have exactly two arguments, the first one is the
// actual argument (or an argument list), while the second one is a reference to the result.
func (n *Node) SayHello(args interface{}, reply *string) {
	// NOTICE: use reply (the second parameter) to pass the return value instead of "return" statements.
	*reply = fmt.Sprintf("Hello %s, I am Node %s", args, n.Identifier)
}

// helper function to print table column name and datatype
/*
   0 - TypeInt32 = iota
   1 - TypeInt64
   2 - TypeFloat
   3 - TypeDouble
   4 - TypeBoolean
   5 - TypeString
*/
func (n *Node) PrintTableColumnSchemas() {
	for _, v := range n.TableMap {
		fmt.Print("\n")
		fmt.Printf("------------ Table %s Columns -------------- \n", v.schema.TableName)
		var columnCount = v.GetColumnCount()
		for i := 0; i < columnCount; i++ {
			fmt.Print(">> ")
			fmt.Println(v.GetColumnName(i) + " " + strconv.Itoa(v.GetColumnType(i)))
		}
		fmt.Print("\n")
	}
}

func (n *Node) BuildTable(args interface{}, reply *string) {
	// TODO: error handling
	_ = n.CreateTable(args.(*TableSchema))

	// uncomment to debug
	//n.PrintTableColumnSchemas()

	*reply = fmt.Sprintf("Successfully built table %s for Node %s", args.(*TableSchema).TableName, n.Identifier)
}

func (n *Node) FragmentWrite(params []interface{}, reply *string) {
	//tableName := params[0]
	//row := params[1]

	tableName := params[0].(string)
	row := params[1].(Row)
	err := n.Insert(tableName, &row)
	if err != nil {
		*reply = fmt.Sprintf("Insertion Error")
	}

	*reply = fmt.Sprintf("Successfully insert row %s into Table %s for Node %s", row, tableName, n.Identifier)
	//*reply = fmt.Sprintf("Successfully insert row  into Table %s\n for Node %s", tableName, n.Identifier)
	//*reply = fmt.Sprintf("Successfully insert row")
}

// CreateTable creates a Table on this node with the provided schema. It returns nil if the table is created
// successfully, or an error if another table with the same name already exists.
func (n *Node) CreateTable(schema *TableSchema) error {
	// check if the table already exists
	if _, ok := n.TableMap[schema.TableName]; ok {
		return errors.New("table already exists")
	}
	// create a table and store it in the map
	t := NewTable(
		schema,
		NewMemoryListRowStore(),
	)
	n.TableMap[schema.TableName] = t
	return nil
}

// Insert inserts a row into the specified table, and returns nil if succeeds or an error if the table does not exist.
func (n *Node) Insert(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Insert(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// Remove removes a row from the specified table, and returns nil if succeeds or an error if the table does not exist.
// It does not concern whether the provided row exists in the table.
func (n *Node) Remove(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Remove(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// IterateTable returns an iterator of the table through which the caller can retrieve all rows in the table in the
// order they are inserted. It returns (iterator, nil) if the Table can be found, or (nil, err) if the Table does not
// exist.
func (n *Node) IterateTable(tableName string) (RowIterator, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.RowIterator(), nil
	} else {
		return nil, errors.New("no such table")
	}
}

// Dataset row will include rowIdx primary key
func (n *Node) GetTableDataset(args interface{}, reply *Dataset) {
	tableName := args.(string)

	if table, ok := n.TableMap[tableName]; ok {
		reply.Schema = *table.schema
		rowIterator, _ := n.IterateTable(tableName)

		for rowIterator.HasNext() {
			reply.Rows = append(reply.Rows, *rowIterator.Next())
		}

	} else {
		reply = nil
	}
}

func (n *Node) TableHasColumn(args []string, reply *bool) {
	// args[0] = table name
	// args[1] = column name
	tableName := args[0]
	columnName := args[1]
	tableSchema := n.TableMap[tableName].schema
	if tableSchema.GetColIndexByName(columnName) == -1 {
		*reply = false
	} else {
		*reply = true
	}
}

func (n *Node) FilterTableWithColumnValues(args []interface{}, reply *Dataset) {
	// args[0] = name of table to be filtered
	// args[1] = column of table that should be filtered on
	// args[2] = hashmap that stores possible column values on other table

	tableName := args[0].(string)
	filterColumnName := args[1].(string)
	filterColumnIndex := int(n.TableMap[tableName].schema.GetColIndexByName(filterColumnName))
	possibleJoinValueSet := args[2].(ValueSet)

	// the table fragment in this node does not has the column
	if filterColumnIndex == -1 {
		reply = nil
		return
	}

	// if table exists
	if table, ok := n.TableMap[tableName]; ok {
		reply.Schema = *table.schema
		rowIterator, _ := n.IterateTable(tableName)

		// get all rows one by one
		for rowIterator.HasNext() {
			// iterator is incremented here
			row := *rowIterator.Next()

			// only add row to reply dataset if the column value exists on other table
			if possibleJoinValueSet[row[filterColumnIndex]] == true {
				reply.Rows = append(reply.Rows, row)
			}
		}

	} else {
		reply = nil
	}

}

func (n *Node) FilterTableWithPKs(args []interface{}, reply *Dataset) {
	// args[0] = tableName
	// args[1...n] list of PKs
	tableName := args[0].(string)
	primaryKeys := args[1:]

	// if table exists
	if table, ok := n.TableMap[tableName]; ok {
		reply.Schema = *table.schema
		rowIterator, _ := n.IterateTable(tableName)

		// get all rows one by one
		for rowIterator.HasNext() {
			// iterator is incremented here
			row := *rowIterator.Next()
			for _, pk := range primaryKeys {
				// filter row by allowed primary key list
				// assume first item of each row is primary key
				if row[0] == pk {
					reply.Rows = append(reply.Rows, row)
				}
			}
		}

	} else {
		reply = nil
	}

}

// IterateTable returns the count of rows in a table. It returns (cnt, nil) if the Table can be found, or (-1, err)
// if the Table does not exist.
func (n *Node) count(tableName string) (int, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.Count(), nil
	} else {
		return -1, errors.New("no such table")
	}
}

// ScanTable returns all rows in a table by the specified name or nothing if it does not exist.
// This method is recommended only to be used for TEST PURPOSE, and try not to use this method in your implementation,
// but you can use it in your own test cases.
// The reason why we deprecate this method is that in practice, every table is so large that you cannot transfer a whole
// table through network all at once, so sending a whole table in one RPC is very impractical. One recommended way is to
// fetch a batch of Rows a time.
func (n *Node) ScanTable(tableName string, dataset *Dataset) {
	if t, ok := n.TableMap[tableName]; ok {
		resultSet := Dataset{}

		tableRows := make([]Row, t.Count())
		i := 0
		iterator := t.RowIterator()
		for iterator.HasNext() {
			// Skip the un-partitioned table row idx of each row
			tableRows[i] = (*iterator.Next())[1:]
			i = i + 1
		}

		resultSet.Rows = tableRows
		resultSet.Schema = *t.schema
		*dataset = resultSet
	}
}
