package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/joshhartwig/gatedrop/internal/database"
	"github.com/joshhartwig/gatedrop/internal/logger"

	_ "github.com/lib/pq"
)

type application struct {
	port        int
	environment string
	debug       bool
	logger      *slog.Logger
	dbUrl       string
	db          *database.Queries
	jwtSecret   string
}

const jwtTokenExiration int = 3600

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("error loading environment variables, quitting server")
		return
	}

	var app application

	// load env vars
	app.dbUrl = os.Getenv("DB_URL")
	app.environment = os.Getenv("APP_ENV")
	app.jwtSecret = os.Getenv("JWT_SECRET")

	// get server port from cmd line
	flag.IntVar(&app.port, "server port", 3244, "api port")
	flag.Parse()

	db, err := sql.Open("postgres", app.dbUrl)
	if err != nil {
		fmt.Println("error opening db, exiting")
		return
	}

	app.db = database.New(db)

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
