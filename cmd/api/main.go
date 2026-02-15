package main

import (
	"log"

	"github.com/farxc/envelopa-transparencia/internal/db"
	"github.com/farxc/envelopa-transparencia/internal/env"
	"github.com/farxc/envelopa-transparencia/internal/store"
)

//	@title			Envelopa Transparência
//	@termsOfService	http://swagger.io/terms/

//	@contact.name	API Support
//	@contact.url	http://www.swagger.io/support
//	@contact.email	support@swagger.io

//	@license.name	Apache 2.0
//	@license.url	http://www.apache.org/licenses/LICENSE-2.0.html

//	@BasePath	/v1
//
// securityDefinitions.apiKey ApiKeyAuth
//
//	@in			header
//	@name		Authorization
//	@description
func main() {
	cfg := config{
		addr: env.GetString("ADDR", ":8080"),
		db: dbConfig{
			addr:         env.GetString("DB_ADDR", "postgres://admin:helloworld@localhost:5454/transparency_wrapper_db?sslmode=disable"),
			maxOpenConns: env.GetInt("DB_MAX_OPEN_CONNS", 25),
			maxIdleConns: env.GetInt("DB_MAX_IDLE_CONNS", 25),
			maxIdleTime:  env.GetString("DB_MAX_IDLE_TIME", "15m"),
		},
		apiUrl: env.GetString("API_URL", "localhost:8080"),
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
