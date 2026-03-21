package main

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"
)

func TestWriteJSONWithHeaders(t *testing.T) {
	var app application
	data := envelope{"message": "success"}

	w := &testResponseWriter{header: http.Header{}}
	headers := http.Header{"X-Custom": []string{"test-value"}}

	err := app.WriteJSON(w, http.StatusCreated, data, headers)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !bytes.Contains(w.Body, []byte(`"message":"success"`)) {
		t.Error("expected JSON data not found in response")
	}

	fmt.Println("body", string(w.Body))
}

func TestWriteJSONContentType(t *testing.T) {
	var app application
	data := envelope{"test": "data"}

	w := &testResponseWriter{header: http.Header{}}
	err := app.WriteJSON(w, http.StatusOK, data, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type: application/json, got: %s", contentType)
	}
}

type testResponseWriter struct {
	header http.Header
	Code   int
	Body   []byte
}

func (w *testResponseWriter) Header() http.Header {
	return w.header
}

func (w *testResponseWriter) Write(b []byte) (int, error) {
	w.Body = append(w.Body, b...)
	return len(b), nil
}

func (w *testResponseWriter) WriteHeader(code int) {
	w.Code = code
}
