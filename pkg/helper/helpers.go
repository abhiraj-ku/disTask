package helper

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
)

func ReadJSON(w http.ResponseWriter, r *http.Request, data any) error {

	doc := json.NewDecoder(r.Body)
	err := doc.Decode(data)
	if err != nil {
		return err
	}

	err = doc.Decode(&struct{}{}) // this means we don't want values other than the required as we have already decoded it
	if err != io.EOF {
		return errors.New("body must have only a single json value")
	}

	return nil
}

// WriteJSON sets the appropriate headers and writes the provided data as JSON to the response
func WriteJSON(w http.ResponseWriter, status int, data any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if data != nil {
		if err := json.NewEncoder(w).Encode(data); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			slog.Error("JSON encoding error", "error", err)
		}
	}
	return nil
}
