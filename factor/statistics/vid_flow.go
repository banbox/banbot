package statistics

import "fmt"

// BipartiteEdge identifies an allowed left/right allocation, independently of
// any monotone price ordering. Quantities use the caller's common unit.
type BipartiteEdge struct{ Left, Right int }
type BipartiteAllocation struct {
	Left, Right int
	Quantity    float64
}
type BipartiteFlow struct {
	Quantity    float64
	Allocations []BipartiteAllocation
}

// MaximumBipartiteFlow implements the Chapter 10 general layered-network
// (Dinic) construction. It proves fill feasibility, not broker execution.
func MaximumBipartiteFlow(left, right []float64, allowed []BipartiteEdge) (BipartiteFlow, error) {
	if len(left)+len(right) > 100000 || len(allowed) > 1000000 {
		return BipartiteFlow{}, fmt.Errorf("statistics: bipartite network exceeds bounds")
	}
	type edge struct {
		to, reverse int
		remaining   float64
	}
	n := len(left) + len(right) + 2
	source, sink := n-2, n-1
	graph := make([][]edge, n)
	add := func(from, to int, capacity float64) int {
		index := len(graph[from])
		graph[from] = append(graph[from], edge{to, len(graph[to]), capacity})
		graph[to] = append(graph[to], edge{from, index, 0})
		return index
	}
	for i, capacity := range left {
		if !finite(capacity) || capacity < 0 {
			return BipartiteFlow{}, fmt.Errorf("statistics: invalid left capacity")
		}
		add(source, i, capacity)
	}
	for i, capacity := range right {
		if !finite(capacity) || capacity < 0 {
			return BipartiteFlow{}, fmt.Errorf("statistics: invalid right capacity")
		}
		add(len(left)+i, sink, capacity)
	}
	indices := make([]int, len(allowed))
	seen := make(map[BipartiteEdge]bool, len(allowed))
	for i, pair := range allowed {
		if pair.Left < 0 || pair.Left >= len(left) || pair.Right < 0 || pair.Right >= len(right) || seen[pair] {
			return BipartiteFlow{}, fmt.Errorf("statistics: invalid or duplicate bipartite edge")
		}
		seen[pair] = true
		capacity := min(left[pair.Left], right[pair.Right])
		indices[i] = add(pair.Left, len(left)+pair.Right, capacity)
	}
	level, cursor := make([]int, n), make([]int, n)
	var push func(int, float64) float64
	push = func(node int, available float64) float64 {
		if node == sink {
			return available
		}
		for cursor[node] < len(graph[node]) {
			i := cursor[node]
			e := graph[node][i]
			if e.remaining > 0 && level[e.to] == level[node]+1 {
				flow := push(e.to, min(available, e.remaining))
				if flow > 0 {
					graph[node][i].remaining -= flow
					graph[e.to][e.reverse].remaining += flow
					return flow
				}
			}
			cursor[node]++
		}
		return 0
	}
	result := BipartiteFlow{}
	for {
		for i := range level {
			level[i], cursor[i] = -1, 0
		}
		level[source] = 0
		queue := []int{source}
		for i := 0; i < len(queue); i++ {
			for _, e := range graph[queue[i]] {
				if e.remaining > 0 && level[e.to] < 0 {
					level[e.to] = level[queue[i]] + 1
					queue = append(queue, e.to)
				}
			}
		}
		if level[sink] < 0 {
			break
		}
		for {
			flow := push(source, 1.7976931348623157e308)
			if flow == 0 {
				break
			}
			result.Quantity += flow
			if !finite(result.Quantity) {
				return BipartiteFlow{}, fmt.Errorf("statistics: network flow overflow")
			}
		}
	}
	for i, pair := range allowed {
		e := graph[pair.Left][indices[i]]
		quantity := graph[e.to][e.reverse].remaining
		if quantity > 0 {
			result.Allocations = append(result.Allocations, BipartiteAllocation{pair.Left, pair.Right, quantity})
		}
	}
	return result, nil
}
