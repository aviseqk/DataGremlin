package postgres

import (
	"context"
	//diagnostics "datagremlin/internals/database/diagnostics"
	"fmt"
	pglogrepl "github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	"log"
	"time"
)

type ReplicationSlotStatus struct {
	SlotName             string
	Plugin               string
	SlotType             string
	Database             string
	Active               bool
	Temporary            string
	RestartLSN           string
	ConfirmedLSN         string
	LastCheckedTimestamp time.Time
}

type SlotCheckpoint struct {
	SlotName  string
	LastLSN   string
	UpdatedAt time.Time
}

type SlotManager struct {
	Slots       map[string]ReplicationSlotStatus
	Checkpoints map[string]SlotCheckpoint
}

func NewSlotManager() *SlotManager {
	return &SlotManager{
		Slots:       make(map[string]ReplicationSlotStatus),
		Checkpoints: make(map[string]SlotCheckpoint),
	}
}

func (sm *SlotManager) CreateNewReplicationSlot(ctx context.Context, conn *pgx.Conn, slotname string, temp bool) (string, error) {
	const outputPlugin = "wal2json"
	options := pglogrepl.CreateReplicationSlotOptions{
		Temporary: temp,
	}

	res, err := pglogrepl.CreateReplicationSlot(ctx, conn.PgConn(), slotname, outputPlugin, options)
	if err != nil {
		return "", fmt.Errorf("error while creating replicationSlot: %w", err)
	}

	fmt.Println("[DEBUG] ", res.SlotName, "|", res.SnapshotName, "|", res.ConsistentPoint, "|", res.OutputPlugin)
	return res.SlotName, nil

}

func (sm *SlotManager) PopulateSlotManager(ctx context.Context, normal_conn *pgx.Conn, repl_conn *pgconn.PgConn) error {
	if err := sm.fetchReplicationSlots(ctx, normal_conn); err != nil {
		fmt.Println(err)
		return err
	}

	//if i == 1 {
	//	createReplicationSlot(ctx, repl_conn, "test_slot")
	//}

	return nil
}

func createReplicationSlot(ctx context.Context, conn *pgx.Conn, slotname string) {
	const outputPlugin = "wal2json"
	options := pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
	}

	res, err := pglogrepl.CreateReplicationSlot(ctx, conn.PgConn(), slotname, outputPlugin, options)
	if err != nil {
		log.Fatal("error while creating replicationSlot", err)
	}
	// TODO: put logic to add this newly created slot into the slotmanager map

	fmt.Println(res.SlotName, "|", res.SnapshotName, "|", res.ConsistentPoint, "|", res.OutputPlugin)
}

func (sm *SlotManager) fetchReplicationSlots(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `select slot_name, plugin, slot_type, database, active,
		restart_lsn, confirmed_flush_lsn from pg_replication_slots;`)

	if err != nil {
		return fmt.Errorf("unable to get replication slots data: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var slot ReplicationSlotStatus
		err := rows.Scan(
			&slot.SlotName,
			&slot.Plugin,
			&slot.SlotType,
			&slot.Database,
			&slot.Active,
			&slot.RestartLSN,
			&slot.ConfirmedLSN)

		if err != nil {
			return err
		}
		slot.LastCheckedTimestamp = time.Now()
		sm.Slots[slot.SlotName] = slot
	}
	return nil

}

func (sm *SlotManager) DisplayReplicationSlots() error {
	for k, v := range sm.Slots {
		fmt.Println(k, "-", v)
	}
	return nil
}
