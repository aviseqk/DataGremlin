package postgres

import (
	"context"
	"strings"
	//diagnostics "datagremlin/internals/database/diagnostics"
	"fmt"
	pglogrepl "github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	"log"
	"os"
)

func GetIdentifySystemResult(ctx context.Context, conn *pgconn.PgConn) (pglogrepl.IdentifySystemResult, error) {
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return pglogrepl.IdentifySystemResult{}, err
	}

	return sysident, nil
}

func StartReplication(ctx context.Context, repl_conn *pgconn.PgConn, slotname, publication string, temp_slot bool) error {
	plugin := "wal2json"

	// have to identify system (REPL mode)  TODO: replace this with my own postgres_diagnostics module functions

	sysident, err := GetIdentifySystemResult(ctx, repl_conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem Failed: %v", err)
	}

	fmt.Printf("SystemID: %s, Timeline: %d, XLogPos: %s DBName=%s\n",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	// check for the replication slot or create a temporary slot
	if temp_slot {
		_, err = pglogrepl.CreateReplicationSlot(ctx, repl_conn, slotname, plugin,
			pglogrepl.CreateReplicationSlotOptions{Temporary: true})

		if err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Fatalf("CreateReplicationSlot Failed: %v", err)
		}
	}
	fmt.Println("ReplicationSlot ready")

	// start replication stream from slot
	err = pglogrepl.StartReplication(ctx, repl_conn, slotname, sysident.XLogPos, pglogrepl.StartReplicationOptions{})

	if err != nil {
		return fmt.Errorf("StartReplication Failed: %w", err)
	}
	fmt.Println("Streaming Started")

	return nil
}

/* func PrepareForReplication(ctx context.Context, conn *pgconn.PgConn) {
	const outputPlugin = "wal2json"
	pub := os.Getenv("PUBLICATION_NAME")

	// drop a publication if it exists
	query := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", pub)
	result, err := conn.Exec(ctx, query)
	if err != nil {
		log.Println("conn.Exec error for DROP PUBLICATION ", err)
	}
	if result.RowsAffected() != 0 {
		log.Fatal("conn result.RowsAffected() failed")
	}
	log.Printf("drop publication %s dropped\n", pub)

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

	//diagnosticsFunc(ctx, conn)

}
*/

func diagnosticsFunc(ctx context.Context, conn *pgx.Conn) {
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
