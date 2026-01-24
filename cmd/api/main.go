package main

import (
	"log"

	"github.com/farxc/transparency_wrapper/internal/db"
	"github.com/farxc/transparency_wrapper/internal/env"
	"github.com/farxc/transparency_wrapper/internal/store"
)

func main() {
	cfg := config{
		addr: env.GetString("ADDR", ":8080"),
		db: dbConfig{
			addr:         env.GetString("DB_ADDR", "postgres://admin:helloworld@localhost:5454/transparency_wrapper_db?sslmode=disable"),
			maxOpenConns: env.GetInt("DB_MAX_OPEN_CONNS", 25),
			maxIdleConns: env.GetInt("DB_MAX_IDLE_CONNS", 25),
			maxIdleTime:  env.GetString("DB_MAX_IDLE_TIME", "15m"),
		},
	}

	db, err := db.New(
		cfg.db.addr,
		cfg.db.maxOpenConns,
		cfg.db.maxIdleConns,
		cfg.db.maxIdleTime)

	if err != nil {
		log.Panic(err)
	}
	defer db.Close()
	log.Printf("Database connection pool established")

	storage := store.NewStorage(db)

	app := &application{
		config: cfg,
		store:  *storage,
	}

	mux := app.mount()

	log.Fatal(app.run(mux))
}
