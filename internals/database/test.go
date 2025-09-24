package postgres

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	connStr := os.Getenv("PG_CONN")
	if connStr == "" {
		log.Fatalf("set PG_CONN, e.g.: postgres://repl:pw@localhost:5432/postgres?replication=database")
	}

	ctx := context.Background()

	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		log.Fatalf("IdentifySystem failed: %v", err)
	}
	fmt.Printf("SystemID=%s Timeline=%d XLogPos=%s DBName=%s\n",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	slot := "go_test_slot"
	plugin := "wal2json"

	// Create temporary slot
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slot, plugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		log.Fatalf("CreateReplicationSlot failed: %v", err)
	}
	fmt.Println("Replication slot ready")

	// Start replication
	if err := pglogrepl.StartReplication(ctx, conn, slot, sysident.XLogPos, pglogrepl.StartReplicationOptions{}); err != nil {
		log.Fatalf("StartReplication failed: %v", err)
	}
	fmt.Println("Started streaming...")

	clientXLogPos := sysident.XLogPos
	lastStatus := time.Now()

	for {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				continue
			}
			log.Fatalf("ReceiveMessage failed: %v", err)
		}

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("unexpected message: %T", msg)
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, _ := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			fmt.Printf("Keepalive: serverWALEnd=%s reply=%v\n", keepalive.ServerWALEnd, keepalive.ReplyRequested)
			if keepalive.ReplyRequested {
				pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
					WALFlushPosition: clientXLogPos,
					WALApplyPosition: clientXLogPos,
					ClientTime:       time.Now(),
				})
			}

		case pglogrepl.XLogDataByteID:
			xld, _ := pglogrepl.ParseXLogData(cd.Data[1:])
			fmt.Printf("XLogData: start=%s end=%s\n", xld.WALStart, xld.ServerWALEnd)
			if len(xld.WALData) > 0 {
				fmt.Println("Payload:", string(xld.WALData))
			}
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

			if time.Since(lastStatus) > 10*time.Second {
				pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
					WALFlushPosition: clientXLogPos,
					WALApplyPosition: clientXLogPos,
					ClientTime:       time.Now(),
				})
				lastStatus = time.Now()
			}
		}
	}
}
