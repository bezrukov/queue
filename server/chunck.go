package server

// Chunk is a peace of data that contains the messages that we written to it.
// It can be incomplete which means that is currently beeing written into.
type Chunk struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
}
