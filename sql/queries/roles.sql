-- name: CreateRole :one
INSERT INTO roles (name, description)
VALUES ($1, $2)
RETURNING id, name, description;

-- name: DeleteRole :exec
DELETE FROM roles
WHERE id = $1;

-- name: GetRoleByID :one
SELECT id, name, description
FROM roles
WHERE id = $1;

-- name: DeleteAllRoles :exec
DELETE FROM roles;