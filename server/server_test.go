package server

import (
	"bytes"
	"testing"
)

func TestCutToLastMessage(t *testing.T) {
	res := []byte("100\n101\n10")

	wantTruncated, wantRest := []byte("100\n101\n"), []byte("10")
	gotTruncated, gotRest, err := cutToLastMessage(res)
	if err != nil {
		t.Errorf("cutToLastMessage(%q): got errror %v; we want no errors", string(res), err)
	}

	if !bytes.Equal(gotTruncated, wantTruncated) || !bytes.Equal(gotRest, wantRest) {
		t.Errorf(
			"cutToLastMessage(%q) got %q, %q; want %q, %q",
			string(res),
			string(gotTruncated),
			string(gotRest),
			string(wantTruncated),
			string(wantRest),
		)
	}
}

func TestCutToLastMessageError(t *testing.T) {
	res := []byte("100000")

	_, _, err := cutToLastMessage(res)
	if err == nil {
		t.Errorf("cutToLastMessage(%q): got no errrors; we want an errors", string(res))
	}
}
