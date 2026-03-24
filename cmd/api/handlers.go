package main

import "net/http"

func (app *application) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	w.Write([]byte("hi"))
}

func (app *application) createUserHandler(w http.ResponseWriter, r *http.Request) {

}
