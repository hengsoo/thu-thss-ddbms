package models

// Segmentation Rule for DBMS
type Rule struct {
	Predicate map[string][]Condition
	Column    []string
}

type Condition struct {
	Op  string
	Val interface{}
}
