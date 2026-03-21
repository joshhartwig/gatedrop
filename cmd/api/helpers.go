package main

import (
	"encoding/json"
	"net/http"
)

type envelope map[string]any

// WriteJSON encodes the provided data as JSON and writes it to the response writer with the given HTTP status code and headers.
// It marshals the data with indentation, appends a newline, sets the response headers, and writes the JSON-encoded envelope to the response.
// Returns an error if JSON marshaling fails.
func (app *application) WriteJSON(w http.ResponseWriter, status int, data envelope, headers http.Header) error {
	js, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		app.logger.Error(err.Error())
		return err
	}

	// add a new line
	js = append(js, '\n')

	// add the headers
	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(js)

	return nil
}
