package main

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
