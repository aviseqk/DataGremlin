package models

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type WAL2JSONChange struct {
	Kind         string   `json:"kind"`
	Schema       string   `json:"schema"`
	Table        string   `json:"table"`
	ColumnNames  []string `json:"columnnames"`
	ColumnValues []any    `json:"columnvalues"`
}

type WAL2JSONMessage struct {
	Change []WAL2JSONChange `json:"change"`
}

type JSONDatabaseEvent struct {
	Event     string           `json:"event"`
	CreatedAt time.Time        `json:"created_at"`
	Table     string           `json:"table"`
	Data      []DatabaseChange `json:"data"`
}

type DatabaseChange struct {
	ColumnName string `json:"column_name"`
	OldValue   string `json:"old_value"`
	NewValue   any    `json:"new_value"`
}

func (data *WAL2JSONMessage) TransformToKafkaEvent() (KafkaEvent, error) {
	change := data.Change

	var event KafkaEvent

	if len(change) > 1 {
		fmt.Printf("[INFO] Multiple changes recorded in one WAL - handle differently\n")
		return KafkaEvent{}, errors.New("multiple_change_record")
	}

	source := change[0]
	event.Operation = EventOperationMap[source.Kind]
	event.Database = source.Schema
	event.Table = source.Table
	event.CreatedAt = time.Now().UTC()

	for k, v := range source.ColumnNames {
		var entry DatabaseChange
		entry.ColumnName = v
		entry.OldValue = "OLD_VALUE"
		entry.NewValue = source.ColumnValues[k]
		event.Data = append(event.Data, entry)
	}

	log.Println("[DEV-DEBUG] transformed to KafkaEvent: ", event)
	return event, nil
}
