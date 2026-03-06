package graph

import (
	"github.com/Neumenon/cowrie/go"
	"github.com/Neumenon/cowrie/go/ld"
)

// Frame field names (stored in FieldDict)
const (
	fieldKind      = "k"  // Event kind
	fieldOp        = "op" // Operation
	fieldID        = "id" // Node/edge ID
	fieldLabels    = "l"  // Labels (array)
	fieldLabel     = "t"  // Edge type/label (single)
	fieldProps     = "p"  // Properties
	fieldFrom      = "f"  // FromID
	fieldTo        = "to" // ToID
	fieldSubject   = "s"  // Triple subject
	fieldPredicate = "pr" // Triple predicate
	fieldObject    = "o"  // Triple object
	fieldGraph     = "g"  // Named graph
	fieldTimestamp = "ts" // Timestamp
	fieldTermKind  = "tk" // Term kind (IRI/BNode/Literal)
	fieldValue     = "v"  // Term value
	fieldDatatype  = "dt" // Datatype
	fieldLang      = "lg" // Language
)

// encodeEvent converts an Event to an Cowrie value.
func encodeEvent(evt Event, header *StreamHeader) *cowrie.Value {
	// Ensure standard fields are in dictionary
	header.AddField(fieldKind)
	header.AddField(fieldOp)

	switch evt.Kind {
	case EventNode:
		return encodeNodeEvent(evt.Node, header)
	case EventEdge:
		return encodeEdgeEvent(evt.Edge, header)
	case EventTriple:
		return encodeTripleEvent(evt.Triple, header)
	default:
		return cowrie.Null()
	}
}

// encodeNodeEvent encodes a NodeEvent to Cowrie.
func encodeNodeEvent(evt *NodeEvent, header *StreamHeader) *cowrie.Value {
	header.AddField(fieldID)
	header.AddField(fieldLabels)
	header.AddField(fieldProps)

	members := []cowrie.Member{
		{Key: fieldKind, Value: cowrie.Int64(int64(EventNode))},
		{Key: fieldOp, Value: cowrie.Int64(int64(evt.Op))},
		{Key: fieldID, Value: cowrie.String(evt.ID)},
	}

	// Labels as label IDs
	if len(evt.Labels) > 0 {
		labelIDs := make([]*cowrie.Value, len(evt.Labels))
		for i, label := range evt.Labels {
			idx := header.AddLabel(label)
			labelIDs[i] = cowrie.Int64(int64(idx))
		}
		members = append(members, cowrie.Member{Key: fieldLabels, Value: cowrie.Array(labelIDs...)})
	}

	// Properties
	if len(evt.Props) > 0 {
		propsVal := encodeProps(evt.Props, header)
		members = append(members, cowrie.Member{Key: fieldProps, Value: propsVal})
	}

	// Timestamp
	if evt.Timestamp > 0 {
		header.AddField(fieldTimestamp)
		members = append(members, cowrie.Member{Key: fieldTimestamp, Value: cowrie.Datetime64(evt.Timestamp)})
	}

	return cowrie.Object(members...)
}

// encodeEdgeEvent encodes an EdgeEvent to Cowrie.
func encodeEdgeEvent(evt *EdgeEvent, header *StreamHeader) *cowrie.Value {
	header.AddField(fieldID)
	header.AddField(fieldLabel)
	header.AddField(fieldFrom)
	header.AddField(fieldTo)
	header.AddField(fieldProps)

	labelIdx := header.AddLabel(evt.Label)

	members := []cowrie.Member{
		{Key: fieldKind, Value: cowrie.Int64(int64(EventEdge))},
		{Key: fieldOp, Value: cowrie.Int64(int64(evt.Op))},
		{Key: fieldLabel, Value: cowrie.Int64(int64(labelIdx))},
		{Key: fieldFrom, Value: cowrie.String(evt.FromID)},
		{Key: fieldTo, Value: cowrie.String(evt.ToID)},
	}

	if evt.ID != "" {
		members = append(members, cowrie.Member{Key: fieldID, Value: cowrie.String(evt.ID)})
	}

	if len(evt.Props) > 0 {
		propsVal := encodeProps(evt.Props, header)
		members = append(members, cowrie.Member{Key: fieldProps, Value: propsVal})
	}

	if evt.Timestamp > 0 {
		header.AddField(fieldTimestamp)
		members = append(members, cowrie.Member{Key: fieldTimestamp, Value: cowrie.Datetime64(evt.Timestamp)})
	}

	return cowrie.Object(members...)
}

// encodeTripleEvent encodes a TripleEvent to Cowrie.
func encodeTripleEvent(evt *TripleEvent, header *StreamHeader) *cowrie.Value {
	header.AddField(fieldSubject)
	header.AddField(fieldPredicate)
	header.AddField(fieldObject)

	predIdx := header.AddPredicate(evt.Predicate)

	members := []cowrie.Member{
		{Key: fieldKind, Value: cowrie.Int64(int64(EventTriple))},
		{Key: fieldOp, Value: cowrie.Int64(int64(evt.Op))},
		{Key: fieldSubject, Value: encodeTerm(evt.Subject, header)},
		{Key: fieldPredicate, Value: cowrie.Int64(int64(predIdx))},
		{Key: fieldObject, Value: encodeTerm(evt.Object, header)},
	}

	if evt.Graph != "" {
		header.AddField(fieldGraph)
		members = append(members, cowrie.Member{Key: fieldGraph, Value: cowrie.String(evt.Graph)})
	}

	if evt.Timestamp > 0 {
		header.AddField(fieldTimestamp)
		members = append(members, cowrie.Member{Key: fieldTimestamp, Value: cowrie.Datetime64(evt.Timestamp)})
	}

	return cowrie.Object(members...)
}

// encodeTerm encodes an RDFTerm to Cowrie.
func encodeTerm(term RDFTerm, header *StreamHeader) *cowrie.Value {
	header.AddField(fieldTermKind)
	header.AddField(fieldValue)

	members := []cowrie.Member{
		{Key: fieldTermKind, Value: cowrie.Int64(int64(term.Kind))},
		{Key: fieldValue, Value: cowrie.String(term.Value)},
	}

	if term.Datatype != "" {
		header.AddField(fieldDatatype)
		members = append(members, cowrie.Member{Key: fieldDatatype, Value: cowrie.String(string(term.Datatype))})
	}

	if term.Lang != "" {
		header.AddField(fieldLang)
		members = append(members, cowrie.Member{Key: fieldLang, Value: cowrie.String(term.Lang)})
	}

	return cowrie.Object(members...)
}

// encodeProps encodes a properties map to Cowrie.
func encodeProps(props map[string]any, header *StreamHeader) *cowrie.Value {
	members := make([]cowrie.Member, 0, len(props))
	for k, v := range props {
		header.AddField(k)
		members = append(members, cowrie.Member{Key: k, Value: encodeAny(v)})
	}
	return cowrie.Object(members...)
}

// encodeAny encodes any Go value to Cowrie.
func encodeAny(v any) *cowrie.Value {
	switch val := v.(type) {
	case nil:
		return cowrie.Null()
	case bool:
		return cowrie.Bool(val)
	case int:
		return cowrie.Int64(int64(val))
	case int64:
		return cowrie.Int64(val)
	case uint64:
		return cowrie.Uint64(val)
	case float64:
		return cowrie.Float64(val)
	case string:
		return cowrie.String(val)
	case []byte:
		return cowrie.Bytes(val)
	case []any:
		items := make([]*cowrie.Value, len(val))
		for i, item := range val {
			items[i] = encodeAny(item)
		}
		return cowrie.Array(items...)
	case map[string]any:
		members := make([]cowrie.Member, 0, len(val))
		for k, v := range val {
			members = append(members, cowrie.Member{Key: k, Value: encodeAny(v)})
		}
		return cowrie.Object(members...)
	default:
		return cowrie.Null()
	}
}

// decodeEvent converts an Cowrie value to an Event.
func decodeEvent(v *cowrie.Value, header *StreamHeader) (*Event, error) {
	if v.Type() != cowrie.TypeObject {
		return nil, ErrInvalidEvent
	}

	kindVal := v.Get(fieldKind)
	if kindVal == nil {
		return nil, ErrInvalidEvent
	}

	kind := EventKind(kindVal.Int64())
	switch kind {
	case EventNode:
		node, err := decodeNodeEvent(v, header)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: EventNode, Node: node}, nil
	case EventEdge:
		edge, err := decodeEdgeEvent(v, header)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: EventEdge, Edge: edge}, nil
	case EventTriple:
		triple, err := decodeTripleEvent(v, header)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: EventTriple, Triple: triple}, nil
	default:
		return nil, ErrInvalidEvent
	}
}

// decodeNodeEvent decodes a NodeEvent from Cowrie.
func decodeNodeEvent(v *cowrie.Value, header *StreamHeader) (*NodeEvent, error) {
	evt := &NodeEvent{
		Props: make(map[string]any),
	}

	if opVal := v.Get(fieldOp); opVal != nil {
		evt.Op = Op(opVal.Int64())
	}

	if idVal := v.Get(fieldID); idVal != nil {
		evt.ID = idVal.String()
	}

	if labelsVal := v.Get(fieldLabels); labelsVal != nil && labelsVal.Type() == cowrie.TypeArray {
		evt.Labels = make([]string, labelsVal.Len())
		for i := 0; i < labelsVal.Len(); i++ {
			labelIdx := int(labelsVal.Index(i).Int64())
			evt.Labels[i] = header.GetLabel(labelIdx)
		}
	}

	if propsVal := v.Get(fieldProps); propsVal != nil {
		evt.Props = decodeProps(propsVal)
	}

	if tsVal := v.Get(fieldTimestamp); tsVal != nil {
		evt.Timestamp = tsVal.Datetime64()
	}

	return evt, nil
}

// decodeEdgeEvent decodes an EdgeEvent from Cowrie.
func decodeEdgeEvent(v *cowrie.Value, header *StreamHeader) (*EdgeEvent, error) {
	evt := &EdgeEvent{
		Props: make(map[string]any),
	}

	if opVal := v.Get(fieldOp); opVal != nil {
		evt.Op = Op(opVal.Int64())
	}

	if idVal := v.Get(fieldID); idVal != nil {
		evt.ID = idVal.String()
	}

	if labelVal := v.Get(fieldLabel); labelVal != nil {
		labelIdx := int(labelVal.Int64())
		evt.Label = header.GetLabel(labelIdx)
	}

	if fromVal := v.Get(fieldFrom); fromVal != nil {
		evt.FromID = fromVal.String()
	}

	if toVal := v.Get(fieldTo); toVal != nil {
		evt.ToID = toVal.String()
	}

	if propsVal := v.Get(fieldProps); propsVal != nil {
		evt.Props = decodeProps(propsVal)
	}

	if tsVal := v.Get(fieldTimestamp); tsVal != nil {
		evt.Timestamp = tsVal.Datetime64()
	}

	return evt, nil
}

// decodeTripleEvent decodes a TripleEvent from Cowrie.
func decodeTripleEvent(v *cowrie.Value, header *StreamHeader) (*TripleEvent, error) {
	evt := &TripleEvent{}

	if opVal := v.Get(fieldOp); opVal != nil {
		evt.Op = Op(opVal.Int64())
	}

	if subjVal := v.Get(fieldSubject); subjVal != nil {
		evt.Subject = decodeTerm(subjVal)
	}

	if predVal := v.Get(fieldPredicate); predVal != nil {
		predIdx := int(predVal.Int64())
		evt.Predicate = header.GetPredicate(predIdx)
	}

	if objVal := v.Get(fieldObject); objVal != nil {
		evt.Object = decodeTerm(objVal)
	}

	if graphVal := v.Get(fieldGraph); graphVal != nil {
		evt.Graph = graphVal.String()
	}

	if tsVal := v.Get(fieldTimestamp); tsVal != nil {
		evt.Timestamp = tsVal.Datetime64()
	}

	return evt, nil
}

// decodeTerm decodes an RDFTerm from Cowrie.
func decodeTerm(v *cowrie.Value) RDFTerm {
	term := RDFTerm{}

	if kindVal := v.Get(fieldTermKind); kindVal != nil {
		term.Kind = TermKind(kindVal.Int64())
	}

	if valVal := v.Get(fieldValue); valVal != nil {
		term.Value = valVal.String()
	}

	if dtVal := v.Get(fieldDatatype); dtVal != nil {
		term.Datatype = ld.IRI(dtVal.String())
	}

	if langVal := v.Get(fieldLang); langVal != nil {
		term.Lang = langVal.String()
	}

	return term
}

// decodeProps decodes a properties map from Cowrie.
func decodeProps(v *cowrie.Value) map[string]any {
	if v.Type() != cowrie.TypeObject {
		return nil
	}

	props := make(map[string]any)
	for _, m := range v.Members() {
		props[m.Key] = decodeAny(m.Value)
	}
	return props
}

// decodeAny decodes any Cowrie value to Go value.
func decodeAny(v *cowrie.Value) any {
	switch v.Type() {
	case cowrie.TypeNull:
		return nil
	case cowrie.TypeBool:
		return v.Bool()
	case cowrie.TypeInt64:
		return v.Int64()
	case cowrie.TypeUint64:
		return v.Uint64()
	case cowrie.TypeFloat64:
		return v.Float64()
	case cowrie.TypeString:
		return v.String()
	case cowrie.TypeBytes:
		return v.Bytes()
	case cowrie.TypeDatetime64:
		return v.Datetime64()
	case cowrie.TypeArray:
		arr := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			arr[i] = decodeAny(v.Index(i))
		}
		return arr
	case cowrie.TypeObject:
		m := make(map[string]any)
		for _, mem := range v.Members() {
			m[mem.Key] = decodeAny(mem.Value)
		}
		return m
	default:
		return nil
	}
}
