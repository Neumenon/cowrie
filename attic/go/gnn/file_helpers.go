package gnn

import (
	"os"
)

// writeFileImpl writes data to a file.
func writeFileImpl(filename string, data []byte) error {
	return os.WriteFile(filename, data, 0644)
}

// readFileImpl reads data from a file.
func readFileImpl(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}
