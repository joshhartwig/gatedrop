package main

import (
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger"
)

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", app.healthHandler)

	// users
	mux.HandleFunc("POST /api/users", app.createUserHandler)

	mux.Handle("GET /swagger/", httpSwagger.WrapHandler)
	return mux
}
