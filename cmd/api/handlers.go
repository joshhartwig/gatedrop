package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
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
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	passwordHash, err := auth.HashPassword(userReq.Password)
	if err != nil {
		app.logger.Error(ErrorHasingPassword.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	userInDb, err := app.db.CreateUser(r.Context(), database.CreateUserParams{
		Username:     userReq.Email,
		PasswordHash: passwordHash,
		Email:        userReq.Email,
	})

	if err != nil {
		app.logger.Error("error creating user in database", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	app.WriteJSON(w, http.StatusCreated, envelope{"data": userInDb}, nil)

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
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadRequest)
		return
	}

	// fetch user in database by email
	user, err := app.db.GetUserByEmail(r.Context(), login.Email)
	if err != nil {
		app.logger.Error("error fetching user by email", "error", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// compare the password hash with the provided password
	err = auth.VerifyPasswordHash(user.PasswordHash, login.Password)
	if err != nil {
		app.logger.Error("error with password hashes", "error", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	if login.Expires_In_Seconds == 0 || login.Expires_In_Seconds > jwtTokenExiration {
		login.Expires_In_Seconds = jwtTokenExiration
	}

	// generate jwt
	jwt, err := auth.CreateJWT(user.ID, app.jwtSecret, time.Duration(jwtTokenExiration))
	if err != nil {
		app.logger.Error("error generating jwt", "error", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// create a refresh token
	refresh, err := auth.MakeRefreshToken()
	if err != nil {
		app.logger.Error("error generating refresh", "error", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// store the token in the database for the user
	tokenInDb, err := app.db.CreateRefreshToken(r.Context(), database.CreateRefreshTokenParams{
		Token:  refresh,
		UserID: user.ID,
	})
	if err != nil {
		app.logger.Error("error creating refresh in database", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
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

// tokenRefreshHandler godoc
// @Summary refresh users jwt token
// @Description refresh jwt token
// @Tags users
// @Accept json
// @Produce json
// @Param payload body CreateUserRequest true "User payload"
// @Success 200 {object} map[string]interface{}
// @Router /refresh [post]
func (app *application) tokenRefreshHandler(w http.ResponseWriter, r *http.Request) {
	token, err := auth.GetBearerToken(r.Header)
	if err != nil {
		app.logger.Error("error getting bearer token from header")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	tokenInDb, err := app.db.GetRefreshByToken(r.Context(), token)
	if err != nil {
		app.logger.Error("error getting refresh token from database")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	jwt, err := auth.CreateJWT(tokenInDb.UserID, app.jwtSecret, time.Duration(jwtTokenExiration))
	if err != nil {
		app.logger.Error("error creating jwt")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	app.WriteJSON(w, http.StatusCreated, envelope{"token": jwt}, nil)
}

// resetDataHandler handles requests to reset all data in the database.
// This endpoint is only available in development environments and will delete all roles,
// refresh tokens, and users from the database.
// Returns an internal server error if called outside of development environment.
// resetUserDataHandler godoc
// @Summary Reset all database data
// @Description Delete all roles, refresh tokens, and users from the database. Only available in development environment.
// @Tags Development
// @Produce json
// @Success 200
// @Failure 500 {object} string "This route can only be ran in development"
// @Router /reset-data [post]
func (app *application) resetUserDataHandler(w http.ResponseWriter, r *http.Request) {
	if app.environment != "development" {
		app.logger.Error("This route can only be ran in development")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	app.db.DeleteAllRoles(r.Context())
	app.db.DeleteAllRefreshTokens(r.Context())
	app.db.DeleteAllUsers(r.Context())

}

// getAllEventsHandler godoc
// @Summary Get all events
// @Description Retrieves all SX and MX events
// @Tags events
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} string "Internal server error"
// @Router /events [get]
func (app *application) getAllEventsHandler(w http.ResponseWriter, r *http.Request) {
	events, err := app.db.GetAllEvents(r.Context())
	if err != nil {
		app.logger.Error("error getting events: " + err.Error())
		http.Error(w, "error getting events", http.StatusInternalServerError)
		return
	}

	app.WriteJSON(w, http.StatusOK, envelope{"events": events}, nil)
}

// getEventsById retrieves an event by its ID from the database and returns it as JSON.
//
// @Summary      Get event by ID
// @Description  Retrieves a single event using its UUID identifier
// @Tags         events
// @Produce      json
// @Param        id   path      string  true  "Event ID (UUID format)"
// @Success      200  {object}  envelope{event=model.Event}  "Event retrieved successfully"
// @Failure      400  {string}  string  "Invalid or malformed UUID"
// @Failure      404  {string}  string  "Event not found"
// @Failure      500  {string}  string  "Internal server error"
// @Router       /events/{id} [get]
func (app *application) getEventsById(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		app.logger.Error("invalid id")
		http.Error(w, "error getting events", http.StatusInternalServerError)
		return
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	event, err := app.db.GetEventById(r.Context(), uid)
	if err != nil {
		app.logger.Error("error fetching event from database")
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	app.WriteJSON(w, http.StatusOK, envelope{"event": event}, nil)

}
