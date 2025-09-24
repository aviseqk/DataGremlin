package models

import "time"

type KafkaEvent struct {
	Operation string           `json:"operation"`
	CreatedAt time.Time        `json:"created_at"`
	Table     string           `json:"table"`
	Database  string           `json:"database"`
	Data      []DatabaseChange `json:"data"`
}

var EventOperationMap = map[string]string{
	"update": "UPDATE_RECORD",
	"delete": "DELETE_RECORD",
	"insert": "INSERT_RECORD",
}
