package models

import (
	"math"
	"math/bits"
	"strings"
)

// Sets the bit at pos in the integer n.
func setBit(n uint64, pos int) uint64 {
	n |= 1 << pos
	return n
}

// get total number of rules (bit == 1)
func getRuleCount (currentState uint64) int{
	return bits.OnesCount64(currentState)
}

// returns cost of adding a candidate to an existing state, cost = length(nodes) / contribution to existing state
func cost(state uint64, candidate uint64) float64 {
	candidateCost := float64(getRuleCount(candidate))
	contribution := float64(getRuleCount(^state & candidate))
	return candidateCost/contribution
}

func findMinCostNode(nodeStrIdToRulesMap map[string]uint64, state uint64) string{
	minCost := math.Inf(1)
	minCostNodeStrId := ""
	for nodeStrId, ruleState := range nodeStrIdToRulesMap{
		if cost(state, ruleState) < minCost {
			minCostNodeStrId = nodeStrId
		}
	}
	return minCostNodeStrId
}

func setCover(nodeRules []NodeRule) []string{

	totalRulesCount := len(nodeRules)

	nodeStrIdToRulesMap := make(map[string]uint64)
	for _, nodeRule := range nodeRules {

		// get a list of nodes that store this rule
		nodeStrIds := strings.Split(nodeRule.NodeIndices, "|")
		// initialize rule exists state using bits for each node, 00100 -> R3 exists
		ruleState := uint64(0)
		// set bit i to one for rule with index i
		setBit(ruleState, nodeRule.Rule.RuleIdx)

		for _, nodeStrId := range nodeStrIds {
			// use OR to update rule exists state
			nodeStrIdToRulesMap[nodeStrId] |= ruleState
		}
	}

	setCoverNodes := make([]string, 0)
	currentState := uint64(0)

	for getRuleCount(currentState) < totalRulesCount {
		candidate := findMinCostNode(nodeStrIdToRulesMap, currentState)
		setCoverNodes = append(setCoverNodes, candidate)
		currentState |= nodeStrIdToRulesMap[candidate]
		delete(nodeStrIdToRulesMap, candidate)
	}

	return setCoverNodes

}
