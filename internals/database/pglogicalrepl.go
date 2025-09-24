package postgres

import (
	"context"
	"fmt"
	"log"
	"os"

	//pglogrepl "github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
)

func PrepareForReplication(ctx context.Context, conn *pgx.Conn) {
	const outputPlugin = "wal2json"
	pub := os.Getenv("PUBLICATION_NAME")
	fmt.Println(pub)

	// drop a publication if it exists
	query := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", pub)
	result, err := conn.Exec(ctx, query)
	if err != nil {
		log.Fatal("conn.Exec error for DROP PUBLICATION ", err)
	}
	if result.RowsAffected() != 0 {
		log.Fatal("conn result.RowsAffected() failed")
	}

	// create a new publication
	query = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", pub)
	result, err = conn.Exec(ctx, query)
	if err != nil {
		log.Fatal("conn.Exec error for CREATE PUBLICATION")
	}
	if r := result.RowsAffected(); r != 0 {
		log.Fatal("conn result.RowsAffected() for create failed")
	}
	log.Printf("create publication %s done", pub)

	diagnostics(ctx, conn)

}

func diagnostics(ctx context.Context, conn *pgx.Conn) {
	pub := os.Getenv("PUBLICATION_NAME")
	query := fmt.Sprintf("SELECT p.pubname,pr.prpubid,c.relname AS published_table FROM pg_publication p JOIN pg_publication_rel pr ON p.oid = pr.prpubid JOIN pg_class c ON pr.prrelid = c.oid WHERE p.pubname = %s;", pub)

	result, err := conn.Exec(ctx, query)
	if err != nil {
		log.Fatal("conn.Exec error for SELECT FROM pg_publication", err)
	}
	if r := result.RowsAffected(); r != 0 {
		log.Fatal("conn result.RowsAffected() for pg_publication", r)
	}

}
