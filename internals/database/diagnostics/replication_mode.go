package postgres_diagnostics

import (
	"context"
	"log"

	pglogrepl "github.com/jackc/pglogrepl"
	pgconn "github.com/jackc/pgx/v5/pgconn"
)

func getIdentifySystemStatus(ctx context.Context, conn *pgconn.PgConn) {
	// check for the IDENTIFY_SYSTEM command
	res, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		log.Fatal("unable to execute diagnostic commands", err)
	}
	connDiag.iden_sys = res

}
