package cowrie

import (
	"sort"
)

// FNV-1a constants
const (
	fnvOffsetBasis64 = 14695981039346656037
	fnvPrime64       = 1099511628211
)

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
	h := uint64(fnvOffsetBasis64)
	h = hashSchema(v, h)
	return h
}

// SchemaFingerprint32 returns the low 32 bits of SchemaFingerprint64.
// Suitable for use as a type ID in stream frames.
func SchemaFingerprint32(v *Value) uint32 {
	return uint32(SchemaFingerprint64(v))
}

// hashSchema recursively computes the FNV-1a hash of a value's schema.
func hashSchema(v *Value, h uint64) uint64 {
	if v == nil {
		return fnvHashByte(h, byte(TypeNull))
	}

	// Hash the type tag
	h = fnvHashByte(h, byte(v.typ))

	switch v.typ {
	case TypeNull, TypeBool, TypeInt64, TypeUint64, TypeFloat64,
		TypeDecimal128, TypeString, TypeBytes, TypeDatetime64,
		TypeUUID128, TypeBigInt:
		// Scalar types: type tag is sufficient

	case TypeArray:
		// For arrays, hash the schema of each element (order matters)
		// Use count as part of schema for fixed-size arrays
		h = fnvHashUint64(h, uint64(len(v.arrayVal)))
		for _, item := range v.arrayVal {
			h = hashSchema(item, h)
		}

	case TypeObject:
		// For objects, sort keys and hash key+schema pairs
		h = fnvHashUint64(h, uint64(len(v.objectVal)))

		// Get sorted keys for deterministic ordering
		sorted := make([]Member, len(v.objectVal))
		copy(sorted, v.objectVal)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Key < sorted[j].Key
		})

		for _, m := range sorted {
			h = fnvHashString(h, m.Key)
			h = hashSchema(m.Value, h)
		}

	case TypeTensor:
		// For tensors, include dtype and rank (but not dims values, which are data)
		h = fnvHashByte(h, byte(v.tensorVal.DType))
		h = fnvHashUint64(h, uint64(len(v.tensorVal.Dims)))

	case TypeTensorRef:
		// TensorRef schema includes store ID
		h = fnvHashByte(h, v.tensorRefVal.StoreID)

	case TypeImage:
		// Image schema includes format
		h = fnvHashByte(h, byte(v.imageVal.Format))

	case TypeAudio:
		// Audio schema includes encoding and channels
		h = fnvHashByte(h, byte(v.audioVal.Encoding))
		h = fnvHashByte(h, v.audioVal.Channels)

	case TypeUnknownExt:
		// Unknown extensions: include ext type in schema
		h = fnvHashUint64(h, v.unknownExtVal.ExtType)
	}

	return h
}

// fnvHashByte adds a single byte to the FNV-1a hash.
func fnvHashByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= fnvPrime64
	return h
}

// fnvHashBytes adds a byte slice to the FNV-1a hash.
func fnvHashBytes(h uint64, data []byte) uint64 {
	for _, b := range data {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}

// fnvHashString adds a string to the FNV-1a hash.
func fnvHashString(h uint64, s string) uint64 {
	// Hash length first to distinguish "" from no field
	h = fnvHashUint64(h, uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

// fnvHashUint64 adds a uint64 to the FNV-1a hash (little-endian bytes).
func fnvHashUint64(h uint64, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h ^= v & 0xFF
		h *= fnvPrime64
		v >>= 8
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
