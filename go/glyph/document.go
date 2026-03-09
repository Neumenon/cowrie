package glyph

import (
	"fmt"
	"strings"
)

// ParseDocument parses a GLYPH document that may contain @pool definitions,
// @schema directives, @tab blocks (top-level or embedded), and pool references.
// It composes the lower-level Parse*, Pool*, and Tabular APIs into a single
// high-level entry point.
//
// Supported input formats:
//
//	@pool.str id=S1 ["a" "b" "c"]
//	{key=^S1:0 other=42}
//
//	@tab _ [col1 col2]
//	|v1|v2|
//	@end
//
//	{messages=@tab _ [content role]\n|hello|user|\n system="prompt"}
//
//	@schema#abc @keys=[k1 k2]\n{#0=v1 #1=v2}
func ParseDocument(input string) (*GValue, error) {
	return ParseDocumentWithRegistries(input, NewSchemaRegistry(), NewPoolRegistry())
}

// ParseDocumentWithRegistries parses a GLYPH document using the given
// schema and pool registries. Pools defined in the document are added to
// the registry, and pool references are resolved before returning.
func ParseDocumentWithRegistries(input string, schemaReg *SchemaRegistry, poolReg *PoolRegistry) (*GValue, error) {
	lines := strings.Split(input, "\n")

	// Collect pool definitions and find the value start
	var valueLines []string
	inPool := false
	var poolBuf strings.Builder

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Check for pool definition start
		if strings.HasPrefix(trimmed, "@pool.") {
			inPool = true
			poolBuf.Reset()
			poolBuf.WriteString(trimmed)

			// Check if it's a single-line pool
			if strings.Contains(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
				pool, err := ParsePool(poolBuf.String())
				if err != nil {
					return nil, fmt.Errorf("parse pool: %w", err)
				}
				poolReg.Define(pool)
				inPool = false
			}
			continue
		}

		if inPool {
			poolBuf.WriteString("\n")
			poolBuf.WriteString(line)

			if strings.HasSuffix(trimmed, "]") {
				pool, err := ParsePool(poolBuf.String())
				if err != nil {
					return nil, fmt.Errorf("parse pool: %w", err)
				}
				poolReg.Define(pool)
				inPool = false
			}
			continue
		}

		if trimmed == "" {
			continue
		}

		valueLines = append(valueLines, line)
	}

	if len(valueLines) == 0 {
		return nil, fmt.Errorf("no value found in input")
	}

	valueStr := strings.Join(valueLines, "\n")

	// Route to the appropriate parser based on content
	var gv *GValue
	var err error

	trimmedValue := strings.TrimSpace(valueStr)

	switch {
	case strings.HasPrefix(trimmedValue, "@tab _"):
		// Top-level @tab block
		gv, err = ParseTabularLoose(valueStr)
		if err != nil {
			return nil, fmt.Errorf("parse tabular: %w", err)
		}

	case strings.Contains(valueStr, "=@tab _"):
		// Map with embedded @tab blocks
		gv, err = parseMapWithEmbeddedTab(valueStr)
		if err != nil {
			return nil, fmt.Errorf("parse embedded tab: %w", err)
		}

	default:
		// Standard parsing with schema registry
		gv, _, err = ParseLoosePayload(valueStr, schemaReg)
		if err != nil {
			return nil, fmt.Errorf("parse value: %w", err)
		}
	}

	// Resolve pool references
	resolved, err := ResolvePoolRefs(gv, poolReg)
	if err != nil {
		return nil, fmt.Errorf("resolve pool refs: %w", err)
	}

	return resolved, nil
}

// ResolvePoolRefs recursively resolves ^S1:N pool references to their pooled values.
func ResolvePoolRefs(gv *GValue, poolReg *PoolRegistry) (*GValue, error) {
	if gv == nil {
		return nil, nil
	}

	// Check if this is a pool reference (stored as a string starting with ^)
	if gv.Type() == TypeStr {
		s, err := gv.AsStr()
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(s, "^") && strings.Contains(s, ":") {
			ref, err := ParsePoolRef(s)
			if err == nil && ref != nil {
				resolved, err := poolReg.Resolve(*ref)
				if err == nil && resolved != nil {
					return resolved, nil
				}
			}
		}
		return gv, nil
	}

	// Recursively resolve containers
	switch gv.Type() {
	case TypeList:
		items, err := gv.AsList()
		if err != nil {
			return nil, err
		}
		resolved := make([]*GValue, len(items))
		for i, item := range items {
			r, err := ResolvePoolRefs(item, poolReg)
			if err != nil {
				return nil, err
			}
			resolved[i] = r
		}
		return List(resolved...), nil

	case TypeMap:
		entries, err := gv.AsMap()
		if err != nil {
			return nil, err
		}
		resolvedEntries := make([]MapEntry, len(entries))
		for i, e := range entries {
			r, err := ResolvePoolRefs(e.Value, poolReg)
			if err != nil {
				return nil, err
			}
			resolvedEntries[i] = MapEntry{Key: e.Key, Value: r}
		}
		return Map(resolvedEntries...), nil

	case TypeStruct:
		sv, err := gv.AsStruct()
		if err != nil {
			return nil, err
		}
		if sv == nil {
			return gv, nil
		}
		resolvedFields := make([]MapEntry, len(sv.Fields))
		for i, f := range sv.Fields {
			r, err := ResolvePoolRefs(f.Value, poolReg)
			if err != nil {
				return nil, err
			}
			resolvedFields[i] = MapEntry{Key: f.Key, Value: r}
		}
		return Struct(sv.TypeName, resolvedFields...), nil
	}

	return gv, nil
}

// parseMapWithEmbeddedTab parses a map that contains embedded @tab blocks.
// Example: {messages=@tab _ [content role]\n|hello|user|\n system="value"}
func parseMapWithEmbeddedTab(input string) (*GValue, error) {
	input = strings.TrimSpace(input)

	if !strings.HasPrefix(input, "{") {
		return nil, fmt.Errorf("expected { at start of map")
	}

	input = input[1:] // Remove leading {

	if !strings.HasSuffix(strings.TrimSpace(input), "}") {
		return nil, fmt.Errorf("expected } at end of map")
	}
	input = strings.TrimSpace(input)
	input = input[:len(input)-1] // Remove trailing }

	var entries []MapEntry
	remaining := input

	for len(remaining) > 0 {
		remaining = strings.TrimSpace(remaining)
		if remaining == "" {
			break
		}

		// Find key=
		eqIdx := strings.Index(remaining, "=")
		if eqIdx < 0 {
			break
		}

		key := strings.TrimSpace(remaining[:eqIdx])
		remaining = remaining[eqIdx+1:]

		// Check if value is @tab
		if strings.HasPrefix(strings.TrimSpace(remaining), "@tab _") {
			tabStart := strings.Index(remaining, "@tab _")
			tabContent := remaining[tabStart:]

			var tabEnd int
			if endIdx := strings.Index(tabContent, "@end"); endIdx >= 0 {
				tabEnd = endIdx + 4
			} else {
				lines := strings.Split(tabContent, "\n")
				foundEnd := false
				for i, line := range lines {
					trimmed := strings.TrimLeft(line, " \t")
					if i > 0 && !strings.HasPrefix(trimmed, "|") && strings.Contains(trimmed, "=") {
						tabEnd = strings.Index(tabContent, "\n"+line)
						if tabEnd < 0 {
							tabEnd = len(tabContent)
						}
						foundEnd = true
						break
					}
				}
				if !foundEnd {
					tabEnd = len(tabContent)
				}
			}

			tabBlock := strings.TrimSpace(tabContent[:tabEnd])
			remaining = strings.TrimSpace(tabContent[tabEnd:])

			tabValue, err := ParseTabularLoose(tabBlock)
			if err != nil {
				return nil, fmt.Errorf("parse @tab for key %s: %w", key, err)
			}

			entries = append(entries, MapEntry{Key: key, Value: tabValue})
		} else {
			// Regular value
			var valueEnd int
			found := false

			for i := 0; i < len(remaining); i++ {
				if remaining[i] == ' ' || remaining[i] == '\n' {
					rest := strings.TrimLeft(remaining[i:], " \t\n")
					if len(rest) > 0 && isDocKeyStart(rest) {
						valueEnd = i
						found = true
						break
					}
				}
			}
			if !found {
				valueEnd = len(remaining)
			}

			valueStr := strings.TrimSpace(remaining[:valueEnd])
			remaining = strings.TrimSpace(remaining[valueEnd:])

			val, _, err := ParseLoosePayload(valueStr, nil)
			if err != nil {
				val = Str(valueStr)
			}

			entries = append(entries, MapEntry{Key: key, Value: val})
		}
	}

	return Map(entries...), nil
}

// isDocKeyStart checks if a string looks like it starts with "key=".
func isDocKeyStart(s string) bool {
	for i, c := range s {
		if c == '=' {
			return i > 0
		}
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			if c == '"' && i == 0 {
				continue
			}
			return false
		}
	}
	return false
}
