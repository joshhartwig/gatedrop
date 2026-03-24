package auth

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHashPassword(t *testing.T) {
	password := "abc123"
	hashed, err := HashPassword(password)
	if err != nil {
		t.Errorf("error hashing password: %v", err)
		t.Fail()
	}

	if err := VerifyPasswordHash(hashed, password); err != nil {
		t.Errorf("error checking password hash: %v", err)
		t.Fail()
	}
}

func TestMakeJWT(t *testing.T) {
	t.Run("test makeJWT creates a JWT", func(t *testing.T) {
		userId := uuid.New()
		jwtSecret := "secret"
		jwt, err := CreateJWT(userId, jwtSecret, time.Hour)
		if err != nil {
			t.Errorf("error creating jwt")
		}

		uuid, err := ValidateJWT(jwt, jwtSecret)
		if err != nil {
			t.Errorf("error creating jwt")
		}

		if userId.String() != uuid.String() {
			t.Errorf("invalid match")
		}
	})
}

func TestGetApiKey(t *testing.T) {
	header := createHeaderHelper("Authorization", "ApiKey 12345")
	wantToken := "12345"

	gotToken, err := GetAPIKey(*header)
	if err != nil {
		t.Errorf("error parsing api key from header: %v\n", err)
	}

	if gotToken != wantToken {
		t.Errorf("got: %v want: %v\n", gotToken, wantToken)
	}
}

func TestGetBearerToken(t *testing.T) {

	t.Run("Fetches Token from header", func(t *testing.T) {
		header := createHeaderHelper("Authorization", "Bearer 12345678")
		wantToken := "12345678"

		gotToken, err := GetBearerToken(*header)
		if err != nil {
			t.Errorf("Error fetching bearer token from header: %v\n", err)
		}

		if gotToken != wantToken {
			t.Errorf("got token:%v is not the same as want token: %v\n", gotToken, wantToken)
		}
	})

	t.Run("Extra spaces around token", func(t *testing.T) {
		header := createHeaderHelper("Authorization", "Bearer 12345678    ")
		wantToken := "12345678"

		gotToken, err := GetBearerToken(*header)
		if err != nil {
			t.Errorf("Error fetching bearer token from header: %v\n", err)
		}

		if gotToken != wantToken {
			t.Errorf("got token:%v is not the same as want token: %v\n", gotToken, wantToken)
		}
	})

	t.Run("Errors on missing 'Authorization' header", func(t *testing.T) {
		header := createHeaderHelper("", "")
		if _, err := GetBearerToken(*header); err == nil {
			t.Errorf("Expected an error from missing Authorization header")
		}
	})

	t.Run("Empty and Misspelled Auth Header", func(t *testing.T) {
		header := createHeaderHelper("Authorization", "")
		header.Add("Authorization", "")
		if _, err := GetBearerToken(*header); err == nil {
			t.Errorf("Expected an error from missing Authorization header %v", err)
		}
	})

	t.Run("Invalid Bearer Token Prefix", func(t *testing.T) {
		// Intentionally providing an invalid "Bearer " prefix to test error handling
		header := createHeaderHelper("Authorization", "Bearer ")
		if _, err := GetBearerToken(*header); err == nil {
			t.Errorf("Expected an error from invalid Authorization header %v", err)
		}
	})

}

func TestMakeRefreshToken(t *testing.T) {
	t.Run("generates a random string", func(t *testing.T) {
		token, err := MakeRefreshToken()
		if err != nil {
			t.Errorf("error generating refresh token %v", err)
		}

		if token == "" {
			t.Errorf("token should not be nil %v", token)
		}
	})
}

// Helper function that creates http headers to use in various tests
func createHeaderHelper(key, value string) *http.Header {
	header := http.Header{}
	header.Add(key, value)
	return &header
}
