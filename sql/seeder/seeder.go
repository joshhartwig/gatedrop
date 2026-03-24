package seeder

import (
	"database/sql"

	"github.com/bxcodec/faker/v3"
)

type Seed struct {
	db *sql.DB
}

// create user data
func (s Seed) UserSeed() error {
	for i := 0; i < 5; i++ {
		stmt, err := s.db.Prepare(`INSERT INTO users(username,email,balance) VALUES(?,?,?)`)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(faker.Name(), faker.Email(), 500)
		if err != nil {
			return err
		}
	}

	return nil
}
