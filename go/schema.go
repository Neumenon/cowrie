package cowrie

import (
	"encoding/binary"
	"sort"
)

// FNV-1a constants
const (
	fnvOffsetBasis64 = 14695981039346656037
	fnvPrime64       = 1099511628211
)

// §4.1 fingerprint codes (FPC) — a namespace DISTINCT from the §2.3 wire tags
// and from any host enum/iota. The fingerprint captures type STRUCTURE only, so
// these codes are fixed by SPEC-v1 §4.1. Mirrors tools/cowrie_ref/model.py FPC.
const (
	fpcNull      = 0x00
	fpcBool      = 0x01
	fpcInt       = 0x02
	fpcUint      = 0x03
	fpcBigInt    = 0x04
	fpcFloat     = 0x05
	fpcDecimal   = 0x06
	fpcString    = 0x07
	fpcBytes     = 0x08
	fpcUUID      = 0x09
	fpcDatetime  = 0x0A
	fpcArray     = 0x0B
	fpcObject    = 0x0C
	fpcTensor    = 0x0D
	fpcBitmask   = 0x0E
	fpcExtension = 0x0F
)

// appendUvarint appends the minimal LEB128 encoding of v (§2.1) to dst. Used by
// the fingerprint grammar — lengths are minimal uvarints, NOT fixed 8 bytes.
func appendUvarint(dst []byte, v uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

// SchemaFingerprint64 computes a 64-bit FNV-1a fingerprint of the value's schema.
// The schema fingerprint captures the type structure (field names, types, tensor metadata)
// but not the actual values. Two values with identical structure produce the same fingerprint.
//
// This is useful for:
// - Type routing in stream protocols
// - Schema-based dispatch
// - Detecting schema drift
// - Fast schema equality checks
//
// The fingerprint uses canonical ordering (sorted keys) for determinism.
func SchemaFingerprint64(v *Value) uint64 {
	// SPEC-v1 §4: FNV-1a-64 over the §4.2 fingerprint-contribution grammar
	// (FPC codes, minimal-LEB128 lengths, dims/count included). This is computed
	// as fnv1a64(appendFP(v)) — the same shape as the oracle's
	// fnv1a64(_fp(value)) in tools/cowrie_ref/fingerprint.py. NOT the old
	// per-node iota/wire-tag scheme (the B4 divergence).
	buf := appendFP(nil, v)
	h := uint64(fnvOffsetBasis64)
	for _, b := range buf {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}

// SchemaFingerprint32 returns the low 32 bits of SchemaFingerprint64.
// Suitable for use as a type ID in stream frames.
func SchemaFingerprint32(v *Value) uint32 {
	return uint32(SchemaFingerprint64(v))
}

// appendFP appends value v's §4.2 fingerprint-contribution bytes to dst and
// returns the extended slice. This is the Go port of tools/cowrie_ref/
// fingerprint.py _fp: type STRUCTURE only, addressed by §4.1 FPC codes, with
// minimal-LEB128 lengths; collection sizes (array len, object count, bitmask
// count) and tensor dtype+rank+dims are structural; values are never included.
func appendFP(dst []byte, v *Value) []byte {
	if v == nil {
		return append(dst, fpcNull)
	}
	switch v.typ {
	case TypeNull:
		return append(dst, fpcNull)
	case TypeBool:
		return append(dst, fpcBool)
	case TypeInt64:
		// §1.1 number domain: an int64-range integer fingerprints as "int".
		return append(dst, fpcInt)
	case TypeUint64:
		return append(dst, fpcUint)
	case TypeBigInt:
		return append(dst, fpcBigInt)
	case TypeFloat64:
		return append(dst, fpcFloat)
	case TypeDecimal128:
		return append(dst, fpcDecimal)
	case TypeString:
		return append(dst, fpcString)
	case TypeBytes:
		return append(dst, fpcBytes)
	case TypeUUID128:
		return append(dst, fpcUUID)
	case TypeDatetime64:
		return append(dst, fpcDatetime)
	case TypeBitmask:
		// §4.2: bitmask count is structural.
		dst = append(dst, fpcBitmask)
		return appendUvarint(dst, v.bitmaskVal.Count)
	case TypeTensor:
		// §4.2: dtype + rank + shape (dims) are type structure.
		dst = append(dst, fpcTensor, byte(v.tensorVal.DType), byte(len(v.tensorVal.Dims)))
		for _, d := range v.tensorVal.Dims {
			dst = appendUvarint(dst, d)
		}
		return dst
	case TypeUnknownExt:
		// §4.2: extType is structure, payload excluded.
		dst = append(dst, fpcExtension)
		return appendUvarint(dst, v.unknownExtVal.ExtType)
	case TypeArray:
		dst = append(dst, fpcArray)
		dst = appendUvarint(dst, uint64(len(v.arrayVal)))
		for _, item := range v.arrayVal {
			dst = appendFP(dst, item)
		}
		return dst
	case TypeObject:
		dst = append(dst, fpcObject)
		dst = appendUvarint(dst, uint64(len(v.objectVal)))
		// Canonical key order: ascending by raw UTF-8 bytes (§2.4). Go string
		// comparison is byte-lexicographic, matching the oracle's sorted_keys.
		sorted := make([]Member, len(v.objectVal))
		copy(sorted, v.objectVal)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
		for _, m := range sorted {
			// key length as minimal LEB128 uvarint + raw UTF-8 key bytes + value fp.
			dst = appendUvarint(dst, uint64(len(m.Key)))
			dst = append(dst, m.Key...)
			dst = appendFP(dst, m.Value)
		}
		return dst
	default:
		// Non-core types (graph types etc.) are §2.3-reserved and cannot appear in
		// a decoded v1 value; contribute only the type tag as a defensive fallback.
		return append(dst, byte(v.typ))
	}
}

// fnvHashBytes adds a byte slice to the FNV-1a hash.
func fnvHashBytes(h uint64, data []byte) uint64 {
	for _, b := range data {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}

// SchemaEquals returns true if two values have structurally identical schemas.
// It recursively compares types, object field names, array element schemas,
// tensor dtype/rank, image format, audio encoding/channels, tensor_ref store ID,
// and unknown-ext type — mirroring exactly what SchemaFingerprint64 hashes.
// Actual values (ints, strings, tensor data, etc.) are not compared.
//
// Use SchemaFingerprintEqual for a faster probabilistic check.
func SchemaEquals(a, b *Value) bool {
	// Handle nil (treated as TypeNull)
	aNil := a == nil
	bNil := b == nil
	if aNil || bNil {
		return aNil && bNil
	}

	if a.typ != b.typ {
		return false
	}

	switch a.typ {
	case TypeNull, TypeBool, TypeInt64, TypeUint64, TypeFloat64,
		TypeDecimal128, TypeString, TypeBytes, TypeDatetime64,
		TypeUUID128, TypeBigInt:
		// Scalar types: type tag is sufficient
		return true

	case TypeArray:
		if len(a.arrayVal) != len(b.arrayVal) {
			return false
		}
		for i := range a.arrayVal {
			if !SchemaEquals(a.arrayVal[i], b.arrayVal[i]) {
				return false
			}
		}
		return true

	case TypeObject:
		if len(a.objectVal) != len(b.objectVal) {
			return false
		}
		// Sort both sides by key (same canonical order as hashSchema)
		aSorted := make([]Member, len(a.objectVal))
		bSorted := make([]Member, len(b.objectVal))
		copy(aSorted, a.objectVal)
		copy(bSorted, b.objectVal)
		sort.Slice(aSorted, func(i, j int) bool { return aSorted[i].Key < aSorted[j].Key })
		sort.Slice(bSorted, func(i, j int) bool { return bSorted[i].Key < bSorted[j].Key })
		for i := range aSorted {
			if aSorted[i].Key != bSorted[i].Key {
				return false
			}
			if !SchemaEquals(aSorted[i].Value, bSorted[i].Value) {
				return false
			}
		}
		return true

	case TypeTensor:
		return a.tensorVal.DType == b.tensorVal.DType &&
			len(a.tensorVal.Dims) == len(b.tensorVal.Dims)

	case TypeTensorRef:
		return a.tensorRefVal.StoreID == b.tensorRefVal.StoreID

	case TypeImage:
		return a.imageVal.Format == b.imageVal.Format

	case TypeAudio:
		return a.audioVal.Encoding == b.audioVal.Encoding &&
			a.audioVal.Channels == b.audioVal.Channels

	case TypeUnknownExt:
		return a.unknownExtVal.ExtType == b.unknownExtVal.ExtType
	}

	// All other types (graph types, bitmask, etc.) have no sub-schema fields:
	// type tag equality (checked above) is sufficient.
	return true
}

// SchemaFingerprintEqual returns true if the two values share the same 64-bit
// FNV-1a schema fingerprint. This is the original (probabilistic) implementation
// of SchemaEquals: fast for large schemas, but carries a 1-in-2^64 collision risk.
// Prefer SchemaEquals for correctness-critical comparisons.
func SchemaFingerprintEqual(a, b *Value) bool {
	return SchemaFingerprint64(a) == SchemaFingerprint64(b)
}

// SchemaDescriptor returns a human-readable string describing the schema.
// Useful for debugging and logging.
func SchemaDescriptor(v *Value) string {
	if v == nil {
		return "null"
	}

	switch v.typ {
	case TypeNull:
		return "null"
	case TypeBool:
		return "bool"
	case TypeInt64:
		return "int64"
	case TypeUint64:
		return "uint64"
	case TypeFloat64:
		return "float64"
	case TypeDecimal128:
		return "decimal128"
	case TypeString:
		return "string"
	case TypeBytes:
		return "bytes"
	case TypeDatetime64:
		return "datetime64"
	case TypeUUID128:
		return "uuid128"
	case TypeBigInt:
		return "bigint"

	case TypeArray:
		if len(v.arrayVal) == 0 {
			return "[]"
		}
		// Show first element type for homogeneous arrays
		return "[" + SchemaDescriptor(v.arrayVal[0]) + ",...]"

	case TypeObject:
		if len(v.objectVal) == 0 {
			return "{}"
		}
		// Show field names
		sorted := make([]string, len(v.objectVal))
		for i, m := range v.objectVal {
			sorted[i] = m.Key
		}
		sort.Strings(sorted)
		return "{" + sorted[0] + ",...}"

	case TypeTensor:
		return "tensor<" + v.tensorVal.DType.String() + ">"
	case TypeTensorRef:
		return "tensor_ref"
	case TypeImage:
		return "image"
	case TypeAudio:
		return "audio"
	case TypeUnknownExt:
		return "ext"
	default:
		return "unknown"
	}
}

// String returns the dtype name for debugging.
func (d DType) String() string {
	switch d {
	case DTypeFloat32:
		return "float32"
	case DTypeFloat16:
		return "float16"
	case DTypeBFloat16:
		return "bfloat16"
	case DTypeFloat64:
		return "float64"
	case DTypeInt8:
		return "int8"
	case DTypeInt16:
		return "int16"
	case DTypeInt32:
		return "int32"
	case DTypeInt64:
		return "int64"
	case DTypeUint8:
		return "uint8"
	case DTypeUint16:
		return "uint16"
	case DTypeUint32:
		return "uint32"
	case DTypeUint64:
		return "uint64"
	case DTypeQINT4:
		return "qint4"
	case DTypeQINT2:
		return "qint2"
	case DTypeQINT3:
		return "qint3"
	case DTypeTernary:
		return "ternary"
	case DTypeBinary:
		return "binary"
	default:
		return "unknown"
	}
}
