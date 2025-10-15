package models

import (
	"time"
)

type RedisCache struct {
	LastLSN        string    `json:"last_lsn"`
	FlushLSN       string    `json:"flush_lsn"`
	Timestamp      time.Time `json:"timestamp"`
	SlotName       string    `json:"slot_name"`
	Publication    string    `json:"publication"`
	SystemID       string    `json:"system_id"`
	Timeline       string    `json:"timeline"`
	DatabaseName   string    `json:"db_name"`
	LastKnownError string    `json:"error"`
}
