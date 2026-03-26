package main

import "time"

// JSON input for new user
type CreateUserRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// User login request
type UserLoginRequest struct {
	Password           string `json:"password"`
	Email              string `json:"email"`
	Expires_In_Seconds int    `json:"expires_in_seconds,omitempty"`
}

type UserLoginResponse struct {
	Email         string    `json:"email"`
	Id            string    `json:"id"`
	Created_At    time.Time `json:"created_at"`
	Updated_At    time.Time `json:"updated_at"`
	Token         string    `json:"token"`
	Refresh_Token string    `json:"refresh_token"`
}
