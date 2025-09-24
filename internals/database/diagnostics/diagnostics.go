package postgres_diagnostics

import (
	"context"
	"log"
	"os"

	pglogrepl "github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
)

type connectionDiagnostics struct {
	iden_sys           pglogrepl.IdentifySystemResult
	primaryPublication string
}

var connDiag connectionDiagnostics

func Check(ctx context.Context, query_conn *pgx.Conn, repl_conn *pgconn.PgConn) {
	pub := os.Getenv("PUBLICATION_NAME")

	c, err := checkIfPublicationExists(ctx, query_conn, pub)
	if err != nil {
		log.Println("error while checking for existing publication")
	} else {
		if c {
			log.Printf("Publication: %s exists \n", pub)
		} else {
			log.Printf("No such publication: %s found\n", pub)
		}
	}

	err = getReplicationSlotStatus(ctx, query_conn)
	if err != nil {
		log.Fatal("getReplicationStatus error ", err)
	}

	err = getPublicationStatus(ctx, query_conn)
	if err != nil {
		log.Fatal("getPublicationStatus error ", err)
	}

	getIdentifySystemStatus(ctx, repl_conn)

}
