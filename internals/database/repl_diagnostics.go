package postgres

import (
	"context"
	"database/sql"
	pgx "github.com/jackc/pgx/v5"
	"log"
)

func (sm *SlotManager) CollectSlotMetrics(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `
		SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, 
		pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_bytes
		FROM pg_replication_slots
	`)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var slotName string
		var active bool
		var restartLSN, confirmedLSN, retainedBytes string
		if err := rows.Scan(&slotName, &active, &restartLSN, &confirmedLSN, &retainedBytes); err != nil {
			return err
		}
		log.Default().Printf("[SLOT METRICS] Slot=%s Active=%v RestartLSN=%s ConfirmedLSN=%s Retained=%s\n",
			slotName, active, restartLSN, confirmedLSN, retainedBytes)
	}
	return rows.Err()

}

func (sm *SlotManager) CollectReplMetrics(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `SELECT pid,
       usename,
       application_name,
       client_addr,
       state,
       sent_lsn,
       write_lsn,
       flush_lsn,
       replay_lsn,
       write_lag,
       flush_lag,
       replay_lag
		FROM pg_stat_replication;
		`)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var pid int32
		var usename, application_name, state string
		var client_addr sql.NullString
		var sent_lsn, write_lsn, flush_lsn, replay_lsn string
		var write_lag, flush_lag, replay_lag sql.NullString

		if err := rows.Scan(&pid, &usename, &application_name, &client_addr, &state,
			&sent_lsn, &write_lsn, &flush_lsn, &replay_lsn, &write_lag, &flush_lag, &replay_lag); err != nil {
			return err
		}
		log.Default().Printf(`[REPL METRICS] Pid: %d, Usename: %s, Application_name: %s, Client_addr: %s, State: %s,
			Sent_lsn: %s, Write_lsn: %s, Flush_lsn: %s, Replay_lsn: %s, Write_lag: %v, Flush_lag: %v, Replay_lag: %v\n`,
			pid, usename, application_name, client_addr, state,
			sent_lsn, write_lsn, flush_lsn, replay_lsn, write_lag, flush_lag, replay_lag)
	}
	return rows.Err()

}
