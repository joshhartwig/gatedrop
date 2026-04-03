package main

import (
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger"
)

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()

	// health
	mux.HandleFunc("/api/health", app.healthHandler)

	// utility
	mux.HandleFunc("/api/reset", app.resetUserDataHandler)

	// users
	mux.HandleFunc("POST /api/users", app.createUserHandler)
	mux.HandleFunc("POST /api/login", app.loginUserHandler)
	mux.HandleFunc("POST /api/refresh", app.tokenRefreshHandler)

	// core data
	mux.HandleFunc("GET /api/events", app.getAllEventsHandler)
	mux.HandleFunc("GET /api/events/{id}", app.getEventsById)

	// swagger
	mux.Handle("GET /swagger/", httpSwagger.WrapHandler)

	return mux
}
