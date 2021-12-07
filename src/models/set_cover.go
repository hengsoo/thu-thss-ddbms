package models

import (
	"math"
	"math/bits"
	"strings"
)

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
func getRuleCount (currentState uint64) int{
	return bits.OnesCount64(currentState)
}

// returns cost of adding a candidate to an existing state, cost = length(nodes) / contribution to existing state
func cost(state uint64, candidate uint64) (float64, uint64) {
	candidateCost := float64(getRuleCount(candidate))
	contributionState := ^state & candidate
	contribution := float64(getRuleCount(contributionState))
	return candidateCost/contribution, contributionState
}

func getContributionRuleIdx(contributionState uint64) []int {
	ruleIndices := make([]int, 0)
	// 64 bits max
	for i := 0; i < 64; i++ {
		if hasBit(contributionState, i) {
			ruleIndices = append(ruleIndices, i)
		}
	}

	return ruleIndices
}

func findMinCostNode(nodeStrIdToRulesMap map[string]uint64, state uint64) (string, []int){
	minCost := math.Inf(1)
	minCostNodeStrId := ""
	var minCostNodeRules []int

	for nodeStrId, ruleState := range nodeStrIdToRulesMap{
		cost, contributionState := cost(state, ruleState)
		if cost < minCost {
			minCost = cost
			minCostNodeStrId = nodeStrId
			minCostNodeRules = getContributionRuleIdx(contributionState)
		}
	}
	return minCostNodeStrId, minCostNodeRules
}

func setCover(nodeRules []NodeRule) map[string][]int{

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

	setCoverNodeToRulesMap := make(map[string][]int)

	currentState := uint64(0)

	for getRuleCount(currentState) < totalRulesCount {
		candidate, rules := findMinCostNode(nodeStrIdToRulesMap, currentState)
		setCoverNodeToRulesMap[candidate] = rules
		currentState |= nodeStrIdToRulesMap[candidate]
		delete(nodeStrIdToRulesMap, candidate)
	}

	return setCoverNodeToRulesMap

}
