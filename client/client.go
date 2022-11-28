package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"

	"github.com/bezrukov/queue/server"
)

const defaultScratchSize = 64 * 1024

// Simple represents an instance of client connected to a set of queue servers.
type Simple struct {
	addrs        []string
	cl           *http.Client
	currentChunk server.Chunk
	off          uint64
}

// NewSimple creates a new client for the queue server.
func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
	}
}

// Send sends the messages to the queue server.
func (s *Simple) Send(msgs []byte) error {
	res, err := s.cl.Post(s.addrs[0]+"/write", "application/octet-string", bytes.NewReader(msgs))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return fmt.Errorf("sending data: http code %d, %s", res.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, res.Body)
	return nil

}

// Receive will either wait for new messages or return an
// error in case something does wrong.
// The scratch buffer cat be used to read the data.
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	addrIdx := rand.Intn(len(s.addrs))
	addr := s.addrs[addrIdx]

	if err := s.updateCurrentChunk(addr); err != nil {
		return nil, fmt.Errorf("updateCurrentChunk error: %w", err)
	}

	readURL := fmt.Sprintf("%s/read?off=%d&maxSize=%d&chunk=%s", addr, s.off, len(scratch), s.currentChunk.Name)

	res, err := s.cl.Get(readURL)
	if err != nil {
		return nil, fmt.Errorf("read %q: %v", readURL, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return nil, fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}

	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return nil, fmt.Errorf("writing response: %v", err)
	}

	// 0 bytes read but no errors means the end of file by convention.
	if b.Len() == 0 {
		if !s.currentChunk.Complete {
			return nil, io.EOF
		}
		if err := s.ackCurrentChunk(addr); err != nil {
			return nil, fmt.Errorf("ack current chunk: %v", err)
		}

		// need to read to next chunk so that we do not return empty response
		s.currentChunk = server.Chunk{}
		s.off = 0
		return s.Receive(scratch)
	}

	s.off += uint64(b.Len())
	return b.Bytes(), nil
}

func (s *Simple) updateCurrentChunk(addr string) error {
	if s.currentChunk.Name != "" {
		return nil
	}

	chunks, err := s.listChunks(addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}

	// need to prioritise the chunks that are complete
	// so that we ack them.
	for _, c := range chunks {
		if c.Complete {
			s.currentChunk = c
			return nil
		}
	}

	s.currentChunk = chunks[0]
	return nil
}

func (s *Simple) listChunks(addr string) ([]server.Chunk, error) {
	listURL := fmt.Sprintf("%s/listChunks", addr)

	resp, err := s.cl.Get(listURL)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("listChunks error: %s", body)
	}

	var res []server.Chunk
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Simple) ackCurrentChunk(addr string) error {
	resp, err := s.cl.Get(fmt.Sprintf(addr+"/ack?chunk=%s", s.currentChunk.Name))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, resp.Body)
	return nil
}
