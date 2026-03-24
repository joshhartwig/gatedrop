package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/joshhartwig/gatedrop/internal/logger"
)

type application struct {
	port        int
	environment string
	debug       bool
	logger      *slog.Logger
	dbUrl       string
	jwtSecret   string
}

const jwtTokenExiration int = 3600

func main() {
	var app application

	err := godotenv.Load()
	if err != nil {
		log.Panic(err)
	}

	// load env vars
	app.dbUrl = os.Getenv("DB_URL")
	app.environment = os.Getenv("APP_ENV")
	app.jwtSecret = os.Getenv("JWT_SECRET")

	// get server port from cmd line
	flag.IntVar(&app.port, "server port", 3244, "api port")

	// create our server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", app.port),
		Handler: app.routes(),
	}

	app.logger = logger.New(app.environment)

	fmt.Printf("starting server on port :%d\n", app.port)
	err = server.ListenAndServe()
	if err != nil {
		fmt.Println(err)
	}
}
