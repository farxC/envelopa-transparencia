package main

import (
	"log"

	"github.com/farxc/transparency_wrapper/internal/data"
	"github.com/farxc/transparency_wrapper/internal/env"
)

func main() {
	cfg := config{
		addr: env.GetString("ADDR", ":8080"),
	}

	// db := sql

	store := data.NewStorage(nil)
	app := &application{
		config: cfg,
		store:  store,
	}

	mux := app.mount()

	log.Fatal(app.run(mux))
}
