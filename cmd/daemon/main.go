package main

import (
	"context"
	"datagremlin/internals/database"
	"fmt"
	"log"
	"os"

	pgx "github.com/jackc/pgx/v5"
)

type Config struct {
	DB_conn *pgx.Conn
	//Kafka *kafka.Client
	//Redis *Redis.Client
}

func main() {
	ctx := context.Background()

	conn, err := postgres.NewPostgresConnection(ctx)
	if err != nil {
		log.Fatal("could not connect to postgres")
		os.Exit(1)
	}

	app := &Config{
		DB_conn: conn,
	}

	fmt.Println("✅ Connected to Postgres!")
	defer app.DB_conn.Close(ctx)

	postgres.PrintAllTables(ctx, app.DB_conn)
	postgres.PrepareForReplication(ctx, app.DB_conn)

}
