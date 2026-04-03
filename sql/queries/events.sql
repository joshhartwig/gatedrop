-- name: GetAllEvents :many
SELECT id, name, starts_at
FROM events
ORDER BY starts_at ASC;

-- name: GetFutureEvents :many
SELECT id, name, starts_at
FROM events
WHERE starts_at >= NOW()
ORDER BY starts_at ASC;

-- name: GetEventById :one
SELECT id, name, starts_at
FROM events
WHERE id = $1;

