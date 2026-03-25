package main

import (
	"encoding/json"
	"net/http"

	"github.com/joshhartwig/gatedrop/internal/auth"
	"github.com/joshhartwig/gatedrop/internal/database"
)

// healthHandler godoc
// @Summary Health check
// @Description Returns API health status
// @Tags health
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /health [get]
func (app *application) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	status := struct {
		Status string `json:"status"`
	}{
		Status: "available",
	}

	app.WriteJSON(w, http.StatusOK, envelope{"data": status}, nil)
}

// createUserHandler godoc
// @Summary Create user
// @Description Creates a new user account
// @Tags users
// @Accept json
// @Produce json
// @Param payload body CreateUserRequest true "User payload"
// @Success 200 {object} map[string]interface{}
// @Router /users [post]
func (app *application) createUserHandler(w http.ResponseWriter, r *http.Request) {
	var userReq CreateUserRequest
	err := json.NewDecoder(r.Body).Decode(&userReq)
	if err != nil {
		app.logger.Error("error decoding json", "error", err)
		return
	}

	passwordHash, err := auth.HashPassword(userReq.Password)
	if err != nil {
		app.logger.Error("error encoding password", "error", err)
		return
	}

	userInDb, err := app.db.CreateUser(r.Context(), database.CreateUserParams{
		Username:     userReq.Email,
		PasswordHash: passwordHash,
		Email:        userReq.Email,
	})

	if err != nil {
		app.logger.Error("error creating user in database", "error", err)
		return
	}

	app.WriteJSON(w, http.StatusOK, envelope{"data": userInDb}, nil)

}
