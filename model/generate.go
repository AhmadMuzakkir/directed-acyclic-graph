package model

import (
	"math/rand"
	"time"
)

// Generate a DAG. It will have size vertices and size - 1 edges
func GenerateGraph(size int) *DAG {
	rand.Seed(time.Now().UnixNano())

	graph := NewDAG()

	// To keep track vertices for each rank.
	rankVertices := make(map[int][]*Vertex)

	rank := 0
	// Keep track of the vertices count for a rank
	rankVertexCount := 0

	for vertexCount := 1; vertexCount <= size; {
		// Generate random ID
		id := randID()

		// Check if ID already exist.
		_, ok := graph.vertices[id]
		if ok {
			continue
		}

		// Create the vertex
		v := NewVertex(id, randBool(), rank)
		graph.vertices[v.ID] = v

		rankVertices[rank] = append(rankVertices[rank], v)

		vertexCount++
		rankVertexCount++

		// Increase the rank. The rules:
		// - First rank should only have one vertex
		// - Last rank should only have one vertex
		// - Min vertices per rank is 10
		// - Max vertices per rank is 30
		rank++
		rankVertexCount = 0
		if rank == 0 || vertexCount == size || (rankVertexCount >= 10 && randBool()) || rankVertexCount > 30 {
			rank++
			rankVertexCount = 0
		}
	}

	// Add a parent to each vertex.
	// The parent of a vertex must have lower rank than the vertex's rank.
	for _, v := range graph.vertices {
		// The first vertex cannot have parents
		if v.Rank == 0 {
			continue
		}
		// Choose a random rank, lower than the vertex' rank
		randomRank := rand.Intn(v.Rank)

		// Get the list of vertices on that rank.
		vertices := rankVertices[randomRank]

		// Choose a random vertice from the list
		randomVertice := randVertex(vertices)

		// Add the random vertex as parent
		// v.Parents = append(v.Parents, randomVertice.ID)
		v.Parents[randomVertice.ID] = struct{}{}

		// Update the children of the random vertex
		randomVertice.Children[v.ID] = struct{}{}
	}

	return graph
}

const letters = "0123456789ABCDEF"

func randID() string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randBool() bool {
	return rand.Intn(2) == 1
}

// Choose a random vertex from the list of vertices
func randVertex(list []*Vertex) *Vertex {
	n := rand.Intn(len(list))
	return list[n]
}
