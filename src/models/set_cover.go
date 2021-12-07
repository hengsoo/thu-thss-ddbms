package models

import (
	"math"
	"math/bits"
	"strings"
)

// Returns true if pos-th bit in n is 1
func hasBit(n uint64, pos int) bool {
	val := n & (1 << pos)
	return val > 0
}

// Sets the bit at pos in the integer n.
func setBit(n uint64, pos int) uint64 {
	n |= 1 << pos
	return n
}

// get total number of rules (bit == 1)
func getRuleCount(currentState uint64) int {
	return bits.OnesCount64(currentState)
}

// returns cost of adding a candidate to an existing state, cost = length(nodes) / contribution to existing state
func cost(state uint64, candidate uint64) (float64, uint64) {
	// if a node has more rules, more traffic cost is required
	candidateCost := float64(getRuleCount(candidate))

	// getRuleCount(~A & B) returns the amount of newly added 1s, in this case amount of newly covered rules
	// Target   Candidate    Truth value (Contribution)
	//    0         0              0
	//    0         1              1
	//    1         0              0
	//    1         1              0
	contributionState := ^state & candidate
	contribution := float64(getRuleCount(contributionState))

	// used float64 to prevent truncation
	// returns contribution state which stores the unique rule IDs covered by this candidate node
	return candidateCost / contribution, contributionState
}

func getContributionRuleIdx(contributionState uint64) []int {
	ruleIndices := make([]int, 0)

	// uint64 has 64 bits
	for i := 0; i < 64; i++ {

		// get all rule IDs exclusively covered by this node
		if hasBit(contributionState, i) {
			ruleIndices = append(ruleIndices, i)
		}
	}

	return ruleIndices
}

func findMinCostNode(nodeStrIdToRulesMap map[string]uint64, state uint64) (string, []int) {
	minCost := math.Inf(1)
	minCostNodeStrId := ""
	var minCostNodeRules []int

	// loop non-deterministically and greedily get node that covers most rule and has less cost
	for nodeStrId, ruleState := range nodeStrIdToRulesMap {
		cost, contributionState := cost(state, ruleState)
		if cost < minCost {
			minCost = cost
			minCostNodeStrId = nodeStrId
			minCostNodeRules = getContributionRuleIdx(contributionState)
		}
	}
	return minCostNodeStrId, minCostNodeRules
}

func setCover(nodeRules []NodeRule) map[string][]int {

	totalRulesCount := len(nodeRules)

	nodeStrIdToRulesMap := make(map[string]uint64)
	for _, nodeRule := range nodeRules {

		// get a list of nodes that store this rule
		nodeStrIds := strings.Split(nodeRule.NodeIndices, "|")

		// initialize rule exists state using bits for each node, 00100 -> R3 exists
		ruleState := uint64(0)

		// set bit i to one for rule with index i
		ruleState = setBit(ruleState, nodeRule.Rule.RuleIdx)

		for _, nodeStrId := range nodeStrIds {
			// use OR to update rule exists state
			nodeStrIdToRulesMap[nodeStrId] |= ruleState
		}
	}

	// Node ID -> Rule index
	setCoverNodeToRulesMap := make(map[string][]int)

	// i-th bit is 0 if rule with index i is not covered
	currentState := uint64(0)

	// loop until all rules are covered
	for getRuleCount(currentState) < totalRulesCount {
		candidate, rules := findMinCostNode(nodeStrIdToRulesMap, currentState)

		// add node that contributes the most to
		setCoverNodeToRulesMap[candidate] = rules

		// add newly covered rules by new node
		currentState |= nodeStrIdToRulesMap[candidate]

		// prevent reentrancy
		delete(nodeStrIdToRulesMap, candidate)
	}

	return setCoverNodeToRulesMap

}
