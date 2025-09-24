package postgres_diagnostics

import (
	"context"
	"fmt"
	"log"

	//pglogrepl "github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
)

func checkIfPublicationExists(ctx context.Context, conn *pgx.Conn, pub string) (bool, error) {

	// check if the publication exists in NORMAL QUERY mode
	var name string

	err := conn.QueryRow(ctx, "select pubname from pg_publication where pubname=$1", pub).Scan(&name)
	if err != nil {
		return false, err
	}

	if name == pub {
		connDiag.primaryPublication = name
		return true, nil
	}
	return false, nil
}

func getPublicationStatus(ctx context.Context, conn *pgx.Conn) error {
	pubsCount := 0
	queryStr := `SELECT * FROM pg_publication;`
	rows, err := conn.Query(ctx, queryStr)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		pubsCount++
		var oid, pubowner uint32
		var pubname string
		var puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot bool
		if err := rows.Scan(&oid, &pubname, &pubowner, &puballtables, &pubinsert, &pubupdate, &pubdelete, &pubtruncate, &pubviaroot); err != nil {
			return err
		}
		fmt.Printf("OID: %d, pubname: %s, pubowner: %d, puballtables: %v, pubinsert: %v, pubdelete: %v, pubupdate: %v, pubtruncate: %v, pubviaroot: %v\n",
			oid, pubname, pubowner, puballtables, pubinsert, pubdelete, pubupdate, pubtruncate, pubviaroot)
	}
	if pubsCount == 0 {
		log.Default().Println("No Publications found within the DB")
		return nil
	}
	return rows.Err()

}

func getReplicationSlotStatus(ctx context.Context, conn *pgx.Conn) error {
	slotsCount := 0
	queryStr := `SELECT slot_name, plugin, slot_type, database, active, restart_lsn,
				confirmed_flush_lsn FROM pg_replication_slots;`
	rows, err := conn.Query(ctx, queryStr)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		slotsCount++
		var slotName, plugin, slotType, database string
		var active bool
		var restartLSN, confirmedLSN string
		if err := rows.Scan(&slotName, &plugin, &slotType, &database, &active, &restartLSN, &confirmedLSN); err != nil {
			return err
		}
		fmt.Printf("Slot: %s, Active: %v, Restart LSN: %s, Confirmed LSN: %s\n",
			slotName, active, restartLSN, confirmedLSN)
	}
	if slotsCount == 0 {
		log.Default().Println("No Replication Slots found within the DB")
		return nil
	}
	return rows.Err()
}
