package pgq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"go.dataddo.com/pgq"
)

type PayloadStruct struct {
	Foo string `json:"foo"`
}

func ExamplePublisher() {
	db, err := sql.Open("postgres", "user=postgres password=postgres host=localhost port=5432 dbname=postgres")
	if err != nil {
		log.Fatal("Error opening database:", err)
	}
	defer db.Close()
	const queueName = "test_queue"
	p := pgq.NewPublisher(db)
	payload, _ := json.Marshal(PayloadStruct{Foo: "bar"})
	messages := []*pgq.MessageOutgoing{
		{
			Metadata: pgq.Metadata{
				"version": "1.0",
			},
			Payload: json.RawMessage(payload),
		},
		{
			Metadata: pgq.Metadata{
				"version": "1.0",
			},
			Payload: json.RawMessage(payload),
		},
		{
			Metadata: pgq.Metadata{
				"version": "1.0",
			},
			Payload: json.RawMessage(payload),
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ids, err := p.Publish(ctx, queueName, messages...)
	if err != nil {
		log.Fatal("Error publishing message:", err)
	}
	log.Println("Published messages with ids:", ids)
}
