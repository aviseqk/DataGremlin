package postgres

import (
	"context"
	"fmt"
	"os"

	//pglogrepl "github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
	getenv "github.com/joho/godotenv"
)

func NewPostgresConnection(ctx context.Context) (*pgx.Conn, error) {

	conn, err := connectToDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	return conn, nil
}

func connectToDB(ctx context.Context) (*pgx.Conn, error) {
	if os.Getenv("ENVIRONMENT") != "production" {
		_ = getenv.Load()
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		os.Getenv("POSTGRES_DB_USERNAME"),
		os.Getenv("POSTGRES_DB_PASSWORD"),
		os.Getenv("POSTGRES_DB_HOST"),
		os.Getenv("POSTGRES_DB_PORT"),
		os.Getenv("POSTGRES_DB_NAME"),
	)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	return conn, nil

}
func PrintAllTables(ctx context.Context, conn *pgx.Conn) error {
	// get all the tables in the current database
	query := "SELECT table_name from information_schema.tables WHERE table_schema='public'"

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var table_name string
		_ = rows.Scan(&table_name)
		fmt.Println("Table: ", table_name)
	}
	return nil
}
