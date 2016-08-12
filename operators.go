package ecql

type increaseType int64
type decreaseType int64

// Inc increases (or decreases) a counter.
func Inc(v int64) increaseType {
	return increaseType(v)
}

// Dec decreases (or increases) a counter.
func Dec(v int64) decreaseType {
	return decreaseType(v)
}
