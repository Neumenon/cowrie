package loader

import (
	"context"
	"fmt"
	"strings"

	"github.com/Neumenon/cowrie/go/graph"
)

// Neo4jConfig holds configuration for Neo4j connection.
type Neo4jConfig struct {
	URI       string // bolt://localhost:7687
	Username  string
	Password  string
	Database  string // empty = default database
	BatchSize int    // events per transaction
}

// Neo4jExecutor is the interface for executing Cypher queries.
// This allows mocking for tests and flexibility in driver implementation.
type Neo4jExecutor interface {
	// Execute runs a Cypher query with parameters.
	Execute(ctx context.Context, query string, params map[string]any) error

	// ExecuteBatch runs multiple queries in a single transaction.
	ExecuteBatch(ctx context.Context, queries []CypherQuery) error

	// Close closes the connection.
	Close() error
}

// CypherQuery represents a parameterized Cypher query.
type CypherQuery struct {
	Query  string
	Params map[string]any
}

// Neo4jWriter writes graph events to Neo4j using Cypher queries.
type Neo4jWriter struct {
	executor  Neo4jExecutor
	ctx       context.Context
	batchSize int

	// Batched queries
	queries []CypherQuery
}

// NewNeo4jWriter creates a new Neo4j writer.
func NewNeo4jWriter(executor Neo4jExecutor, batchSize int) *Neo4jWriter {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &Neo4jWriter{
		executor:  executor,
		ctx:       context.Background(),
		batchSize: batchSize,
		queries:   make([]CypherQuery, 0, batchSize),
	}
}

// WithContext sets the context for database operations.
func (nw *Neo4jWriter) WithContext(ctx context.Context) *Neo4jWriter {
	nw.ctx = ctx
	return nw
}

// WriteNode writes a node event to Neo4j.
func (nw *Neo4jWriter) WriteNode(evt *graph.NodeEvent) error {
	var query CypherQuery

	if evt.Op == graph.OpDelete {
		query = CypherQuery{
			Query:  "MATCH (n {id: $id}) DETACH DELETE n",
			Params: map[string]any{"id": evt.ID},
		}
	} else {
		// Build MERGE query with labels
		query = buildNodeUpsert(evt)
	}

	nw.queries = append(nw.queries, query)

	if len(nw.queries) >= nw.batchSize {
		return nw.Flush()
	}
	return nil
}

// WriteEdge writes an edge event to Neo4j.
func (nw *Neo4jWriter) WriteEdge(evt *graph.EdgeEvent) error {
	var query CypherQuery

	if evt.Op == graph.OpDelete {
		if evt.ID != "" {
			query = CypherQuery{
				Query:  fmt.Sprintf("MATCH ()-[r:%s {id: $id}]->() DELETE r", sanitizeLabel(evt.Label)),
				Params: map[string]any{"id": evt.ID},
			}
		} else {
			query = CypherQuery{
				Query:  fmt.Sprintf("MATCH (a {id: $from})-[r:%s]->(b {id: $to}) DELETE r", sanitizeLabel(evt.Label)),
				Params: map[string]any{"from": evt.FromID, "to": evt.ToID},
			}
		}
	} else {
		query = buildEdgeUpsert(evt)
	}

	nw.queries = append(nw.queries, query)

	if len(nw.queries) >= nw.batchSize {
		return nw.Flush()
	}
	return nil
}

// WriteTriple writes an RDF triple event.
// For Neo4j, we model triples as edges with the predicate as the relationship type.
func (nw *Neo4jWriter) WriteTriple(evt *graph.TripleEvent) error {
	// Convert triple to edge-like structure
	// Subject → Predicate → Object

	var query CypherQuery

	// Extract predicate local name for relationship type
	relType := predicateToRelType(evt.Predicate)

	if evt.Op == graph.OpDelete {
		query = CypherQuery{
			Query: fmt.Sprintf(`
				MATCH (s)-[r:%s]->(o)
				WHERE s.iri = $subj AND o.iri = $obj
				DELETE r
			`, sanitizeLabel(relType)),
			Params: map[string]any{
				"subj": evt.Subject.Value,
				"obj":  evt.Object.Value,
			},
		}
	} else {
		query = buildTripleUpsert(evt, relType)
	}

	nw.queries = append(nw.queries, query)

	if len(nw.queries) >= nw.batchSize {
		return nw.Flush()
	}
	return nil
}

// Flush executes all buffered queries.
func (nw *Neo4jWriter) Flush() error {
	if len(nw.queries) == 0 {
		return nil
	}

	if err := nw.executor.ExecuteBatch(nw.ctx, nw.queries); err != nil {
		return err
	}

	nw.queries = nw.queries[:0]
	return nil
}

// Close flushes and closes the writer.
func (nw *Neo4jWriter) Close() error {
	if err := nw.Flush(); err != nil {
		return err
	}
	return nw.executor.Close()
}

// buildNodeUpsert creates a MERGE query for a node.
func buildNodeUpsert(evt *graph.NodeEvent) CypherQuery {
	// Build label string
	labelStr := ""
	if len(evt.Labels) > 0 {
		labels := make([]string, len(evt.Labels))
		for i, l := range evt.Labels {
			labels[i] = sanitizeLabel(l)
		}
		labelStr = ":" + strings.Join(labels, ":")
	}

	// Build SET clause for properties
	setClause := "n.id = $id"
	params := map[string]any{"id": evt.ID}

	for k, v := range evt.Props {
		paramName := "p_" + sanitizeParamName(k)
		setClause += fmt.Sprintf(", n.%s = $%s", sanitizeProperty(k), paramName)
		params[paramName] = v
	}

	query := fmt.Sprintf(`
		MERGE (n%s {id: $id})
		SET %s
	`, labelStr, setClause)

	return CypherQuery{Query: query, Params: params}
}

// buildEdgeUpsert creates a MERGE query for an edge.
func buildEdgeUpsert(evt *graph.EdgeEvent) CypherQuery {
	relType := sanitizeLabel(evt.Label)

	params := map[string]any{
		"from": evt.FromID,
		"to":   evt.ToID,
	}

	// Build SET clause for properties
	setClause := ""
	if len(evt.Props) > 0 {
		setClauses := make([]string, 0, len(evt.Props))
		for k, v := range evt.Props {
			paramName := "p_" + sanitizeParamName(k)
			setClauses = append(setClauses, fmt.Sprintf("r.%s = $%s", sanitizeProperty(k), paramName))
			params[paramName] = v
		}
		setClause = "SET " + strings.Join(setClauses, ", ")
	}

	if evt.ID != "" {
		params["rid"] = evt.ID
		setClause = "SET r.id = $rid" + strings.TrimPrefix(setClause, "SET")
	}

	query := fmt.Sprintf(`
		MATCH (a {id: $from}), (b {id: $to})
		MERGE (a)-[r:%s]->(b)
		%s
	`, relType, setClause)

	return CypherQuery{Query: query, Params: params}
}

// buildTripleUpsert creates a MERGE query for an RDF triple.
func buildTripleUpsert(evt *graph.TripleEvent, relType string) CypherQuery {
	params := map[string]any{}

	// Subject node
	subjMatch := buildTermMatch("s", evt.Subject, params, "subj")

	// Object node (may be literal)
	objMatch := buildTermMatch("o", evt.Object, params, "obj")

	query := fmt.Sprintf(`
		%s
		%s
		MERGE (s)-[r:%s]->(o)
	`, subjMatch, objMatch, sanitizeLabel(relType))

	return CypherQuery{Query: query, Params: params}
}

// buildTermMatch creates a MERGE clause for an RDF term.
func buildTermMatch(varName string, term graph.RDFTerm, params map[string]any, prefix string) string {
	switch term.Kind {
	case graph.TermIRI:
		params[prefix+"_iri"] = term.Value
		return fmt.Sprintf("MERGE (%s:Resource {iri: $%s_iri})", varName, prefix)
	case graph.TermBNode:
		params[prefix+"_id"] = term.Value
		return fmt.Sprintf("MERGE (%s:BNode {id: $%s_id})", varName, prefix)
	case graph.TermLiteral:
		params[prefix+"_val"] = term.Value
		if term.Lang != "" {
			params[prefix+"_lang"] = term.Lang
			return fmt.Sprintf("MERGE (%s:Literal {value: $%s_val, lang: $%s_lang})", varName, prefix, prefix)
		}
		if term.Datatype != "" {
			params[prefix+"_dt"] = string(term.Datatype)
			return fmt.Sprintf("MERGE (%s:Literal {value: $%s_val, datatype: $%s_dt})", varName, prefix, prefix)
		}
		return fmt.Sprintf("MERGE (%s:Literal {value: $%s_val})", varName, prefix)
	default:
		params[prefix+"_val"] = term.Value
		return fmt.Sprintf("MERGE (%s {value: $%s_val})", varName, prefix)
	}
}

// sanitizeLabel ensures a label is valid for Neo4j.
func sanitizeLabel(label string) string {
	// Replace invalid characters with underscores
	result := strings.Builder{}
	for i, r := range label {
		if i == 0 {
			if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r == '_' {
				result.WriteRune(r)
			} else {
				result.WriteRune('_')
			}
		} else {
			if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
				result.WriteRune(r)
			} else {
				result.WriteRune('_')
			}
		}
	}
	if result.Len() == 0 {
		return "_"
	}
	return result.String()
}

// sanitizeProperty ensures a property name is valid.
func sanitizeProperty(name string) string {
	return sanitizeLabel(name)
}

// sanitizeParamName ensures a parameter name is valid.
func sanitizeParamName(name string) string {
	return sanitizeLabel(name)
}

// predicateToRelType extracts a relationship type from a predicate IRI.
func predicateToRelType(predicate string) string {
	// Try to extract local name from IRI
	// e.g., "http://schema.org/knows" → "knows"
	//       "http://xmlns.com/foaf/0.1/name" → "name"

	if idx := strings.LastIndex(predicate, "#"); idx != -1 {
		return predicate[idx+1:]
	}
	if idx := strings.LastIndex(predicate, "/"); idx != -1 {
		return predicate[idx+1:]
	}
	return predicate
}

// MockNeo4jExecutor is a mock executor for testing.
type MockNeo4jExecutor struct {
	Queries []CypherQuery
}

// Execute records a query.
func (m *MockNeo4jExecutor) Execute(ctx context.Context, query string, params map[string]any) error {
	m.Queries = append(m.Queries, CypherQuery{Query: query, Params: params})
	return nil
}

// ExecuteBatch records all queries.
func (m *MockNeo4jExecutor) ExecuteBatch(ctx context.Context, queries []CypherQuery) error {
	m.Queries = append(m.Queries, queries...)
	return nil
}

// Close is a no-op.
func (m *MockNeo4jExecutor) Close() error {
	return nil
}
