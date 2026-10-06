package env

import (
	"errors"
	"io/fs"

	"github.com/joho/godotenv"
)

// Load reads variables from .env in the working directory, if present.
// Variables already set in the environment take precedence.
func Load() error {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
