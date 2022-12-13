package server

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// TODO: limit the max message size too
const readBlockSize = 1024 * 1024
const maxFileChunkSize = 20 * 1024 * 1024 // bytes

// OnDisk stores all the data on disk
type OnDisk struct {
	dirname string

	sync.RWMutex
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
	fps           map[string]*os.File
}

// NewOnDisk creates a server that stores all it's data on dick
func NewOnDisk(dirname string) *OnDisk {
	return &OnDisk{
		dirname: dirname,
		fps:     make(map[string]*os.File),
	}
}

// Write accepts the messages from the clients and stores them
func (s *OnDisk) Write(msgs []byte) error {
	s.Lock()
	defer s.Unlock()

	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxInMemoryChunkSize) {
		s.lastChunk = fmt.Sprintf("chunk%d", s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++
	}

	fp, err := s.getFileDescriptor(s.lastChunk)
	if err != nil {
		return fmt.Errorf("can't get file: %q, err: %w", s.lastChunk, err)
	}

	_, err = fp.Write(msgs)
	if err != nil {
		return fmt.Errorf("can't write to file: %w", err)
	}
	s.lastChunkSize += uint64(len(msgs))
	return nil
}

func (s *OnDisk) getFileDescriptor(chunk string) (*os.File, error) {
	fp, ok := s.fps[chunk]
	if ok {
		return fp, nil
	}

	path := filepath.Join(s.dirname, chunk)

	fp, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("could not create file %q: %w", path, err)
	}

	s.fps[chunk] = fp
	return fp, nil
}

// Read copies the data from the in-memoru store and writes
// the data read to the provider Writer, starting with the
// offset provider
func (s *OnDisk) Read(chunk string, off uint64, maxSize uint64, w io.Writer) error {
	s.Lock()
	defer s.Unlock()

	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	buf := make([]byte, maxSize)
	fp, err := s.getFileDescriptor(chunk)
	if err != nil {
		return fmt.Errorf("getFileDescriptor (%q): %v", chunk, err)
	}

	n, err := fp.ReadAt(buf, int64(off))

	// ReadAt returns EOF wver when it did read some data.
	if n == 0 {
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}

	// Read until the last message.
	// Do not send the incomplete part of the last
	// message if it is cut half.
	truncated, _, err := cutToLastMessage(buf[0:n])
	if err != nil {
		return err
	}

	if _, err := w.Write(truncated); err != nil {
		return err
	}

	return nil
}

// ListChunks returns the list of chunks that are present in the system.
func (s *OnDisk) ListChunks() ([]Chunk, error) {
	res := make([]Chunk, 0, len(s.fps))

	for chunk := range s.fps {
		var ch Chunk

		ch.Complete = (chunk != s.lastChunk)
		ch.Name = chunk
		res = append(res, ch)
	}

	return res, nil
}

func (s *OnDisk) Ack(chunk string) error {
	s.Lock()
	defer s.Unlock()

	if chunk == s.lastChunk {
		return fmt.Errorf("chunk %q is currently being written into and can't be akcnowleged", chunk)
	}

	chunkFileName := filepath.Join(s.dirname, chunk)

	err := os.Remove(chunkFileName)
	if err != nil {
		return fmt.Errorf("could'n remove file: %q err: %w", chunk, err)
	}

	fp, ok := s.fps[chunk]
	if ok {
		fp.Close()
	}

	delete(s.fps, chunk)

	return nil
}
