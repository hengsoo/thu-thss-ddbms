package models

// Segmentation Rule for DBMS
type Rule struct {
	RuleIdx	int
	Predicate map[string][]Condition
	Column    []string
}

type Condition struct {
	Op  string
	Val interface{}
}
