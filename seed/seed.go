package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/joshhartwig/gatedrop/internal/auth"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env: %v", err)
	}

	connStr := os.Getenv("DB_URL")
	if connStr == "" {
		log.Fatal("DB_URL is not set")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("error pinging database: %v", err)
	}

	ctx := context.Background()

	log.Println("seeding roles...")
	adminRoleID := upsertRole(ctx, db, "admin", "Application administrator")
	userRoleID := upsertRole(ctx, db, "user", "Standard application user")

	log.Println("seeding classes...")
	class450ID := upsertClass(ctx, db, "450SX")
	class250ID := upsertClass(ctx, db, "250SX")

	log.Println("seeding season...")
	seasonID := getOrCreateSeason(ctx, db, "2026 AMA Supercross", "2026-01-10", "2026-05-10", true)

	log.Println("seeding events...")
	event1ID := getOrCreateEvent(ctx, db, seasonID, "Anaheim 1", "2026-01-10 19:00:00")
	event2ID := getOrCreateEvent(ctx, db, seasonID, "Glendale", "2026-01-17 19:00:00")
	event3ID := getOrCreateEvent(ctx, db, seasonID, "San Diego", "2026-01-24 19:00:00")

	log.Println("seeding riders...")
	// 450SX riders
	seedRider(ctx, db, "Chase", "Sexton", class450ID)
	seedRider(ctx, db, "Eli", "Tomac", class450ID)
	seedRider(ctx, db, "Ken", "Roczen", class450ID)
	seedRider(ctx, db, "Jason", "Anderson", class450ID)
	seedRider(ctx, db, "Cooper", "Webb", class450ID)
	// 250SX riders
	seedRider(ctx, db, "Haiden", "Deegan", class250ID)
	seedRider(ctx, db, "Tom", "Vialle", class250ID)
	seedRider(ctx, db, "Nate", "Thrasher", class250ID)
	seedRider(ctx, db, "Jordon", "Smith", class250ID)
	seedRider(ctx, db, "Pierce", "Brown", class250ID)

	log.Println("seeding event classes...")
	for _, eventID := range []string{event1ID, event2ID, event3ID} {
		upsertEventClass(ctx, db, eventID, class450ID)
		upsertEventClass(ctx, db, eventID, class250ID)
	}

	log.Println("seeding users...")
	adminID := upsertUser(ctx, db, "admin", "admin@gatedrop.com", "Admin@1234!", 1000)
	aliceID := upsertUser(ctx, db, "alice", "alice@gatedrop.com", "Alice@1234!", 500)
	bobID := upsertUser(ctx, db, "bob", "bob@gatedrop.com", "Bob@1234!", 500)

	log.Println("seeding user roles...")
	upsertUserRole(ctx, db, adminID, adminRoleID)
	upsertUserRole(ctx, db, aliceID, userRoleID)
	upsertUserRole(ctx, db, bobID, userRoleID)

	log.Println("seed complete!")
}

func upsertRole(ctx context.Context, db *sql.DB, name, description string) string {
	const q = `
		INSERT INTO roles (name, description) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description
		RETURNING id`
	var id string
	if err := db.QueryRowContext(ctx, q, name, description).Scan(&id); err != nil {
		log.Fatalf("upsertRole %q: %v", name, err)
	}
	return id
}

func upsertClass(ctx context.Context, db *sql.DB, code string) string {
	const q = `
		INSERT INTO classes (code) VALUES ($1)
		ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code
		RETURNING id`
	var id string
	if err := db.QueryRowContext(ctx, q, code).Scan(&id); err != nil {
		log.Fatalf("upsertClass %q: %v", code, err)
	}
	return id
}

func getOrCreateSeason(ctx context.Context, db *sql.DB, name, startsAt, endsAt string, isActive bool) string {
	var id string
	err := db.QueryRowContext(ctx, `SELECT id FROM seasons WHERE name = $1`, name).Scan(&id)
	if err == nil {
		return id
	}
	if err != sql.ErrNoRows {
		log.Fatalf("getOrCreateSeason select %q: %v", name, err)
	}
	const ins = `INSERT INTO seasons (name, starts_at, ends_at, is_active) VALUES ($1, $2, $3, $4) RETURNING id`
	if err := db.QueryRowContext(ctx, ins, name, startsAt, endsAt, isActive).Scan(&id); err != nil {
		log.Fatalf("getOrCreateSeason insert %q: %v", name, err)
	}
	return id
}

func getOrCreateEvent(ctx context.Context, db *sql.DB, seasonID, name, startsAt string) string {
	var id string
	err := db.QueryRowContext(ctx,
		`SELECT id FROM events WHERE season_id = $1 AND name = $2`, seasonID, name,
	).Scan(&id)
	if err == nil {
		return id
	}
	if err != sql.ErrNoRows {
		log.Fatalf("getOrCreateEvent select %q: %v", name, err)
	}
	const ins = `INSERT INTO events (season_id, name, starts_at) VALUES ($1, $2, $3) RETURNING id`
	if err := db.QueryRowContext(ctx, ins, seasonID, name, startsAt).Scan(&id); err != nil {
		log.Fatalf("getOrCreateEvent insert %q: %v", name, err)
	}
	return id
}

func seedRider(ctx context.Context, db *sql.DB, firstName, lastName, classID string) {
	var id string
	err := db.QueryRowContext(ctx,
		`SELECT id FROM riders WHERE first_name = $1 AND last_name = $2 AND class_id = $3`,
		firstName, lastName, classID,
	).Scan(&id)
	if err == nil {
		return
	}
	if err != sql.ErrNoRows {
		log.Fatalf("seedRider select %s %s: %v", firstName, lastName, err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO riders (first_name, last_name, class_id) VALUES ($1, $2, $3)`,
		firstName, lastName, classID,
	); err != nil {
		log.Fatalf("seedRider insert %s %s: %v", firstName, lastName, err)
	}
}

func upsertEventClass(ctx context.Context, db *sql.DB, eventID, classID string) {
	const q = `INSERT INTO event_classes (event_id, class_id) VALUES ($1, $2) ON CONFLICT (event_id, class_id) DO NOTHING`
	if _, err := db.ExecContext(ctx, q, eventID, classID); err != nil {
		log.Fatalf("upsertEventClass: %v", err)
	}
}

func upsertUser(ctx context.Context, db *sql.DB, username, email, password string, balance int) string {
	hash, err := auth.HashPassword(password)
	if err != nil {
		log.Fatalf("upsertUser hash %q: %v", username, err)
	}
	const q = `
		INSERT INTO users (username, email, password_hash, balance) VALUES ($1, $2, $3, $4)
		ON CONFLICT (email) DO UPDATE
			SET username      = EXCLUDED.username,
			    password_hash = EXCLUDED.password_hash,
			    balance       = EXCLUDED.balance,
			    updated_at    = NOW()
		RETURNING id`
	var id string
	if err := db.QueryRowContext(ctx, q, username, email, hash, balance).Scan(&id); err != nil {
		log.Fatalf("upsertUser %q: %v", username, err)
	}
	return id
}

func upsertUserRole(ctx context.Context, db *sql.DB, userID, roleID string) {
	const q = `INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2) ON CONFLICT (user_id, role_id) DO NOTHING`
	if _, err := db.ExecContext(ctx, q, userID, roleID); err != nil {
		log.Fatalf("upsertUserRole: %v", err)
	}
}
