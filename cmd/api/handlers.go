package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/joshhartwig/gatedrop/internal/auth"
	"github.com/joshhartwig/gatedrop/internal/database"
)

var (
	ErrorCreatingUser   = errors.New("error creating user in database")
	ErrorHasingPassword = errors.New("error hashing password")
	ErrorDecodingJSON   = errors.New("error decoding reqeust body JSON")
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
		app.logger.Error(ErrorHasingPassword.Error())
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

// loginUserHandler godoc
// @Summary login user
// @Description Logs in a user
// @Tags users
// @Accept json
// @Produce json
// @Param payload body CreateUserRequest true "User payload"
// @Success 200 {object} map[string]interface{}
// @Router /login [post]
func (app *application) loginUserHandler(w http.ResponseWriter, r *http.Request) {
	var login UserLoginRequest
	err := json.NewDecoder(r.Body).Decode(&login)
	if err != nil {
		app.logger.Error(ErrorDecodingJSON.Error())
		return
	}

	// fetch user in database by email
	user, err := app.db.GetUserByEmail(r.Context(), login.Email)
	if err != nil {
		app.logger.Error("error fetching user by email", "error", err)
		return
	}

	// compare the password hash with the provided password
	err = auth.VerifyPasswordHash(user.PasswordHash, login.Password)
	if err != nil {
		app.logger.Error("error with password hashes", "error", err)
		return
	}

	if login.Expires_In_Seconds == 0 || login.Expires_In_Seconds > jwtTokenExiration {
		login.Expires_In_Seconds = jwtTokenExiration
	}

	// generate jwt
	jwt, err := auth.CreateJWT(user.ID, app.jwtSecret, time.Duration(jwtTokenExiration))
	if err != nil {
		app.logger.Error("error generating jwt", "error", err)
		return
	}

	// create a refresh token
	refresh, err := auth.MakeRefreshToken()
	if err != nil {
		app.logger.Error("error generating refresh", "error", err)
		return
	}

	// store the token in the database for the user
	tokenInDb, err := app.db.CreateRefreshToken(r.Context(), database.CreateRefreshTokenParams{
		Token:  refresh,
		UserID: user.ID,
	})
	if err != nil {
		app.logger.Error("error creating refresh in database", "error", err)
		return
	}

	userLoginResponse := UserLoginResponse{
		Email:         user.Email,
		Id:            user.ID.String(),
		Created_At:    user.CreatedAt,
		Updated_At:    user.UpdatedAt,
		Token:         jwt,
		Refresh_Token: tokenInDb.Token,
	}

	app.WriteJSON(w, http.StatusAccepted, envelope{"data": userLoginResponse}, nil)

}
