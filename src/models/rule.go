package models

// Segmentation Rule for DBMS
type Rule struct {
	Predicate map[string][]Predicate
	Column []string
}

type Predicate struct {
	Op string
	Val interface{}
}
