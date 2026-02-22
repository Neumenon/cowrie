package stream

import (
	"hash/crc32"
)

// crcTable is the IEEE CRC-32 table.
var crcTable = crc32.MakeTable(crc32.IEEE)

// ComputeCRC computes CRC-32 IEEE of the given bytes.
func ComputeCRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// VerifyCRC verifies that the CRC matches.
func VerifyCRC(data []byte, expected uint32) bool {
	return ComputeCRC(data) == expected
}
