package main

import (
	"context"
	"datagremlin/internals/database"
	"datagremlin/internals/datamodels"
	"datagremlin/internals/kafka"
	"datagremlin/internals/redis"
	//"github.com/jackc/pgproto3/v2"
	"strings"
	"time"
	//diagnostics "datagremlin/internals/database/diagnostics"
	"encoding/json"
	"fmt"
	getenv "github.com/joho/godotenv"
	rdb "github.com/redis/go-redis/v9"
	"log"
	"os"

	"github.com/IBM/sarama"
	pglogrepl "github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	pgproto3 "github.com/jackc/pgx/v5/pgproto3"
)

type Config struct {
	DB_conn_normal *pgx.Conn
	DB_conn_repl   *pgconn.PgConn
	Kafka          sarama.SyncProducer
	Redis          *rdb.Client
}

func main() {
	_ = getenv.Load()
	ctx := context.Background()
	var sysident pglogrepl.IdentifySystemResult

	pub := os.Getenv("PUBLICATION_NAME")
	//dev_slot := os.Getenv("DEV_SLOT_NAME")
	normal_conn, err := postgres.NewPostgresQueryConnection(ctx)
	if err != nil {
		log.Fatal("could not connect to postgres")
		os.Exit(1)
	}

	repl_conn, err := postgres.NewPostgresReplConnection(ctx)

	if err != nil {
		log.Fatal("could not connect to postgres")
		os.Exit(1)
	}

	app := &Config{
		DB_conn_normal: normal_conn,
		DB_conn_repl:   repl_conn,
	}

	fmt.Println("✅ Connected to Postgres!")
	defer app.DB_conn_normal.Close(ctx)
	defer app.DB_conn_repl.Close(ctx)

	sm := postgres.NewSlotManager()
	err = sm.PopulateSlotManager(ctx, app.DB_conn_normal, app.DB_conn_repl)
	if err != nil {
		log.Fatalf("SlotManager Failed: %v", err)
	}

	// init Kafka Connection
	producer, err := kafka.CreateProducer()
	if err != nil {
		log.Fatalf("Unable to build kafka connection: %v", err)
	}
	app.Kafka = producer
	defer producer.Close()

	if err = kafka.SendStringMessage(app.Kafka, "test message from main.go", "test-topic"); err != nil {
		fmt.Printf("unable to produce kafka message")
	}

	// init Redis Connection
	redis_cl, err := redis.GetRedisClient(ctx)
	if err != nil {
		log.Fatalf("Unable to establish Redis Connection: %v", err)
	}
	app.Redis = redis_cl
	if ok, err := redis.TestRedisConnectivity(app.Redis, ctx); err != nil || !ok {
		log.Fatalf("[FAILURE] Redis Connectivity failed: %v", err)
	}

	sm.DisplayReplicationSlots()
	sysident, err = postgres.GetIdentifySystemResult(ctx, app.DB_conn_repl)
	if err != nil {
		log.Fatalf("IdentifySystem Failed: %v", err)
	}
	fmt.Println("Identify System Result", sysident)

	err = postgres.StartReplication(ctx, app.DB_conn_repl, "test_slot", pub, false)
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}

	//clientXLogPos := sysident.XLogPos
	//lastStatus := time.Now()

	for {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rawMsg, err := app.DB_conn_repl.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				continue
			}
			log.Fatalf("Receive Message failed: %v", err)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			fmt.Printf("Unexpected Message: %T\n", rawMsg)
		}
		if len(msg.Data) == 0 {
			continue
		}

		// check message type by first byte
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, _ := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			fmt.Printf("Keepalive: %+v\n", keepalive)
			if keepalive.ReplyRequested {
				pglogrepl.SendStandbyStatusUpdate(ctx, app.DB_conn_repl, pglogrepl.StandbyStatusUpdate{})
			}

		case pglogrepl.XLogDataByteID:
			xld, _ := pglogrepl.ParseXLogData(msg.Data[1:])
			//fmt.Printf("WAL @%s: %s\n", xld.WALStart, string(xld.WALData))
			// optionally unmarshal JSON if using wal2json
			var transformedData models.WAL2JSONMessage
			if err := json.Unmarshal(xld.WALData, &transformedData); err != nil {
				log.Printf("failed to decode WAL JSON: %v\n", err)
			}
			event, err := transformedData.TransformToKafkaEvent()
			if err != nil {
				log.Printf("failed to transform WAL2Json to KafkaEvent %v\n", err)
			}
			log.Println("[DEV-DEBUG] main - transform to Kafka", event)

			// send json to kafka
			err = kafka.SendKafkaEventMessage(producer, event)
			if err != nil {
				log.Printf("failed to send JSON to kafka: %v\n", err)
			} else {
				// TODO: update redis about the WAL Message being processed properly
				var redis_cache = models.RedisCache{
					LastLSN:      xld.WALStart.String(),
					Timestamp:    time.Now(),
					SlotName:     "test-slot",
					SystemID:     sysident.SystemID,
					DatabaseName: sysident.DBName,
					Publication:  pub,
				}
				if err := redis.SaveLSNCacheToRedis(context.Background(), app.Redis, redis_cache); err != nil {
					log.Printf("[FAILURE] Unable to cache LSN into Redis\n")
				} else {
					// TODO: inform Postgres Replication Slot about LSN being consumed properly
					// TEST REMOVE LATER the GetSavedLSNCache Part
					if _, err := redis.GetSavedLSNCache(context.Background(), app.Redis); err != nil {
						fmt.Printf("[FAILURE] could not restore saved Redis LSN Cache\n")
					}

					log.Printf("[SUCCESS] WAL Record with LSN %s processed properly\n", xld.WALStart)
				}
			}

			var change map[string]any
			json.Unmarshal(xld.WALData, &change)
			fmt.Printf("Decoded: %+v\n", change)

			// update the postgres server that this daemon has processed the WAL Update
			// either periodically using a separate GoRoutine or after every message is processed
			lastLSN := xld.WALStart
			// send a StandbyStatusUpdate
			status := pglogrepl.StandbyStatusUpdate{
				WALWritePosition: lastLSN,
				WALFlushPosition: lastLSN,
				WALApplyPosition: lastLSN,
				ClientTime:       time.Now(),
			}
			if err = pglogrepl.SendStandbyStatusUpdate(ctx, app.DB_conn_repl, status); err != nil {
				log.Printf("Send StatusUpdate failed: %v", err)
			}

			// NOTE: separate goroutine method -> do when Kafka included, and have a daemon manager for tracking LSN, for reading, restarting and updating

			/* go func() {
				ticker := time.NewTicker(10 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					status := pglogrepl.StandbyStatusUpdate{
						WALWritePosition: lastLSN,
						WALFlushPosition: lastLSN,
						WALApplyPosition: lastLSN,
						ClientTime:       time.Now(),
					}
					err := pglogrepl.SendStandbyStatusUpdate(ctx, app.DB_conn_repl, status)
					if err != nil {
						log.Printf("Failed to send standby status: %v", err)
					}
				}
			}()
			*/

		default:
			fmt.Printf("Unknown CopyData type: %c\n", msg.Data[0])
		}
	}

	//postgres.PrintAllTables(ctx, app.DB_conn_normal)
	//postgres.PrepareForReplication(ctx, app.DB_conn_repl)

	//	diagnostics.Check(ctx, app.DB_conn_normal, app.DB_conn_repl)

	//sm := postgres.NewSlotManager()
	//err = sm.TestSlotManager(ctx, app.DB_conn_normal, app.DB_conn_repl)
	//if err != nil {
	//	fmt.Println("TestSlotManager error", err)
	//}

	// create one more replication slot
	//str, err := sm.CreateNewReplicationSlot(ctx, app.DB_conn_repl, "test_slot_2", true)
	//if str != "test_slot_2" || err != nil {
	//	log.Fatal("cant create new replication slot")
	//}
	//sm.DisplayReplicationSlots()

	//sm.CollectReplMetrics(ctx, app.DB_conn_normal)
	//sm.CollectSlotMetrics(ctx, app.DB_conn_normal)

}
