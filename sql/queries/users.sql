-- name: CreateUser :one
INSERT INTO users (email, username, password_hash, created_at, updated_at)
VALUES ($1, $2, $3, NOW(), NOW())
RETURNING id, email, username, created_at, updated_at;

-- name: GetUserByEmail :one
SELECT id, email, username, password_hash, created_at, updated_at
FROM users
WHERE email = $1;

-- name: GetUserByID :one
SELECT id, email, username, password_hash, created_at, updated_at
FROM users
WHERE id = $1;

-- name: UpdateUser :one
UPDATE users
SET email = $2, username = $3, password_hash = $4, updated_at = NOW()
WHERE id = $1
RETURNING id, email, username, password_hash, created_at, updated_at;

-- name: DeleteUser :exec
DELETE FROM users
WHERE id = $1;

-- name: ListUsers :many
SELECT id, email, username, created_at, updated_at
FROM users
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- name: DeleteAllUsers :exec
DELETE FROM users;

