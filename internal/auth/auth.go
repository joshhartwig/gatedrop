package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

const bcryptCost = 14

type CustomClaims struct {
	Sub string `json:"sub"`
	jwt.RegisteredClaims
}

// HashPassword generates a bcrypt hash of the provided password string.
func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(bytes), err
}

// VerifyPasswordHash compares a bcrypt hash with a plaintext password.
func VerifyPasswordHash(hash, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

// CreateJWT generates a signed JWT token for the given user ID.
//
// It creates a JWT token with HS256 signing method and includes the user ID as the subject.
// The token is issued by "gatedrop" and expires after the specified duration.
//
// Parameters:
//   - userId: The UUID of the user for whom the token is being created.
//   - tokenSecret: The secret key used to sign the token. Must not be empty.
//   - expiresIn: The duration after which the token will expire.
//
// Returns:
//   - A signed JWT token string if successful.
//   - An error if tokenSecret is empty or if token signing fails.
func CreateJWT(userId uuid.UUID, tokenSecret string, expiresIn time.Duration) (string, error) {
	if tokenSecret == "" {
		return "", errors.New("invalid token secret")
	}

	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		jwt.RegisteredClaims{
			Issuer:    "gatedrop",
			IssuedAt:  jwt.NewNumericDate(time.Now().UTC()),
			ExpiresAt: jwt.NewNumericDate(time.Now().UTC().Add(expiresIn)),
			Subject:   userId.String(),
		})

	signed, err := token.SignedString([]byte(tokenSecret))
	if err != nil {
		return "", errors.New("error signing token")
	}

	return signed, nil
}

// ValidateJWT validates a JWT token and extracts the user ID from its claims.
// It verifies the token's signature using the provided secret, ensures the token
// is valid, and parses the subject claim as a UUID.
//
// Parameters:
//   - tokenString: the JWT token to validate
//   - tokenSecret: the secret key used to verify the token's HMAC signature
//
// Returns:
//   - uuid.UUID: the user ID extracted from the token's subject claim
//   - error: an error if token parsing fails, the token is invalid, the subject
//     claim is missing, or the subject cannot be parsed as a UUID
func ValidateJWT(tokenString, tokenSecret string) (uuid.UUID, error) {
	claims := &CustomClaims{}
	// fetch the token
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected error signing method: %v", token.Header["alg"])
		}
		return []byte(tokenSecret), nil
	})

	if err != nil {
		return uuid.Nil, err
	}

	if !token.Valid {
		return uuid.Nil, errors.New("invalid token")
	}

	subject := claims.Sub
	if subject == "" {
		return uuid.Nil, errors.New("subject nil in claims field")
	}

	id, err := uuid.Parse(subject)
	if err != nil {
		return uuid.Nil, errors.New("error parsing subject")
	}
	return id, nil
}

// GetBearerToken extracts and validates a Bearer token from the Authorization header.
// It expects the header value to follow the format "Bearer <token>".
// Returns the cleaned token string if valid, or an error if the header is missing,
// empty, or does not follow the Bearer token format.
func GetBearerToken(headers http.Header) (string, error) {
	const bearerPrefix = "Bearer "
	token := headers.Get("Authorization")
	if token == "" {
		return "", errors.New("unable to find a token named 'Authorization")
	}

	if strings.HasPrefix(token, bearerPrefix) {
		cleanedToken := strings.TrimSpace(token[len(bearerPrefix):])
		if cleanedToken == "" {
			return "", errors.New("empty token")
		}
		return cleanedToken, nil
	}

	return "", errors.New("invalid bearer token format")
}

// MakeRefreshToken generates a 32 bit hexadecimal string token
func MakeRefreshToken() (string, error) {
	key := make([]byte, 32)
	rand.Read(key)
	return hex.EncodeToString(key), nil
}

// GetApiKey extracts and api key token from Authorization header
func GetAPIKey(headers http.Header) (string, error) {
	const apiPrefix = "ApiKey "
	token := headers.Get("Authorization")
	if token == "" {
		return "", errors.New("unable to find a token named 'Authorization")
	}

	if strings.HasPrefix(token, apiPrefix) {
		cleanedToken := strings.TrimSpace(token[len(apiPrefix):])
		if cleanedToken == "" {
			return "", errors.New("empty token")
		}
		return cleanedToken, nil
	}

	return "", errors.New("invalid bearer token format")
}
