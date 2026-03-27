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
	mux.HandleFunc("/api/reset", app.resetDataHandler)

	// users
	mux.HandleFunc("POST /api/users", app.createUserHandler)
	mux.HandleFunc("POST /api/login", app.loginUserHandler)
	mux.HandleFunc("POST /api/refresh", app.tokenRefreshHandler)

	// swagger
	mux.Handle("GET /swagger/", httpSwagger.WrapHandler)

	return mux
}
