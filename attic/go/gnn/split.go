package gnn

// SplitWriter writes train/val/test splits.
type SplitWriter struct {
	enc *SectionEncoder
}

// NewSplitWriter creates a new split writer.
func NewSplitWriter() *SplitWriter {
	return &SplitWriter{
		enc: NewSectionEncoder(),
	}
}

// WriteIndices writes splits as index arrays.
func (w *SplitWriter) WriteIndices(train, val, test []int64) {
	// Mode: indices
	w.enc.buf.writeByte(byte(SplitModeIndices))

	// Train indices
	w.enc.WriteInt64Array(train)

	// Val indices
	w.enc.WriteInt64Array(val)

	// Test indices
	w.enc.WriteInt64Array(test)
}

// WriteMasks writes splits as bitmasks.
// Each mask is ceil(numNodes/8) bytes.
func (w *SplitWriter) WriteMasks(numNodes int64, trainMask, valMask, testMask []byte) {
	// Mode: mask
	w.enc.buf.writeByte(byte(SplitModeMask))

	// Num nodes
	w.enc.WriteUvarint(uint64(numNodes))

	// Train mask
	w.enc.WriteBytes(trainMask)

	// Val mask
	w.enc.WriteBytes(valMask)

	// Test mask
	w.enc.WriteBytes(testMask)
}

// Bytes returns the encoded section body.
func (w *SplitWriter) Bytes() []byte {
	return w.enc.Bytes()
}

// ToSection returns this as a Section.
func (w *SplitWriter) ToSection(name string) Section {
	return Section{
		Kind: SectionSplit,
		Name: name,
		Body: w.Bytes(),
	}
}

// SplitReader reads train/val/test splits.
type SplitReader struct {
	dec      *SectionDecoder
	mode     SplitMode
	numNodes int64 // For mask mode
}

// NewSplitReader creates a reader for a split section.
func NewSplitReader(data []byte) (*SplitReader, error) {
	dec := NewSectionDecoder(data)

	// Read mode
	modeByte, err := dec.r.readByte()
	if err != nil {
		return nil, err
	}

	r := &SplitReader{
		dec:  dec,
		mode: SplitMode(modeByte),
	}

	// Read num nodes for mask mode
	if r.mode == SplitModeMask {
		numNodes, err := dec.ReadUvarint()
		if err != nil {
			return nil, err
		}
		r.numNodes = int64(numNodes)
	}

	return r, nil
}

// Mode returns the split mode.
func (r *SplitReader) Mode() SplitMode {
	return r.mode
}

// NumNodes returns the number of nodes (mask mode only).
func (r *SplitReader) NumNodes() int64 {
	return r.numNodes
}

// ReadIndices reads the split as index arrays.
func (r *SplitReader) ReadIndices() (train, val, test []int64, err error) {
	train, err = r.dec.ReadInt64Array()
	if err != nil {
		return nil, nil, nil, err
	}

	val, err = r.dec.ReadInt64Array()
	if err != nil {
		return nil, nil, nil, err
	}

	test, err = r.dec.ReadInt64Array()
	if err != nil {
		return nil, nil, nil, err
	}

	return train, val, test, nil
}

// ReadMasks reads the split as bitmasks.
func (r *SplitReader) ReadMasks() (trainMask, valMask, testMask []byte, err error) {
	trainMask, err = r.dec.ReadBytes()
	if err != nil {
		return nil, nil, nil, err
	}

	valMask, err = r.dec.ReadBytes()
	if err != nil {
		return nil, nil, nil, err
	}

	testMask, err = r.dec.ReadBytes()
	if err != nil {
		return nil, nil, nil, err
	}

	return trainMask, valMask, testMask, nil
}

// ReadSplitData reads the full split data.
func (r *SplitReader) ReadSplitData() (*SplitData, error) {
	data := &SplitData{Mode: r.mode}

	if r.mode == SplitModeIndices {
		train, val, test, err := r.ReadIndices()
		if err != nil {
			return nil, err
		}
		data.TrainIdx = train
		data.ValIdx = val
		data.TestIdx = test
	} else {
		trainMask, valMask, testMask, err := r.ReadMasks()
		if err != nil {
			return nil, err
		}
		data.TrainMask = trainMask
		data.ValMask = valMask
		data.TestMask = testMask
	}

	return data, nil
}

// IndicesToMask converts index array to bitmask.
func IndicesToMask(indices []int64, numNodes int64) []byte {
	maskLen := (numNodes + 7) / 8
	mask := make([]byte, maskLen)
	for _, idx := range indices {
		if idx >= 0 && idx < numNodes {
			byteIdx := idx / 8
			bitIdx := uint(idx % 8)
			mask[byteIdx] |= 1 << bitIdx
		}
	}
	return mask
}

// MaskToIndices converts bitmask to index array.
func MaskToIndices(mask []byte, numNodes int64) []int64 {
	var indices []int64
	for i := int64(0); i < numNodes; i++ {
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		if int(byteIdx) < len(mask) && mask[byteIdx]&(1<<bitIdx) != 0 {
			indices = append(indices, i)
		}
	}
	return indices
}

// TrainValTestSplit creates a random split from a list of node IDs.
// trainRatio + valRatio + testRatio should equal 1.0
func TrainValTestSplit(nodeIDs []int64, trainRatio, valRatio float64) (train, val, test []int64) {
	n := len(nodeIDs)
	trainEnd := int(float64(n) * trainRatio)
	valEnd := trainEnd + int(float64(n)*valRatio)

	train = make([]int64, trainEnd)
	val = make([]int64, valEnd-trainEnd)
	test = make([]int64, n-valEnd)

	copy(train, nodeIDs[:trainEnd])
	copy(val, nodeIDs[trainEnd:valEnd])
	copy(test, nodeIDs[valEnd:])

	return train, val, test
}
