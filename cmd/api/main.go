package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/joshhartwig/gatedrop/internal/logger"
)

type application struct {
	port        int
	environment string
	debug       bool
	logger      *slog.Logger
}

func main() {
	var app application
	flag.IntVar(&app.port, "server port", 3244, "api port")
	flag.StringVar(&app.environment, "environment", "development", "environment")
	flag.BoolVar(&app.debug, "debug", true, "true|false")

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", app.port),
		Handler: app.routes(),
	}

	app.logger = logger.New(app.environment)

	fmt.Printf("starting server on port :%d\n", app.port)
	err := server.ListenAndServe()
	if err != nil {
		fmt.Println(err)
	}
}
