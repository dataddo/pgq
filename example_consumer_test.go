package pgq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"

	"go.dataddo.com/pgq"
)

type Handler struct{}

func (h *Handler) HandleMessage(ctx context.Context, msg *pgq.MessageIncoming) (res bool, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		log.Println("Recovered in 'Handler.HandleMessage()'", r)
		// nack the message, it will be retried
		res = pgq.MessageNotProcessed
		if e, ok := r.(error); ok {
			err = e
		} else {
			err = fmt.Errorf("%v", r)
		}
	}()
	if msg.Metadata["heaviness"] == "heavy" {
		// nack the message, it will be retried
		// Message won't contain error detail in the database.
		return pgq.MessageNotProcessed, nil
	}
	var myPayload struct {
		Foo string `json:"foo"`
	}
	if err := json.Unmarshal(msg.Payload, &myPayload); err != nil {
		// discard the message, it will not be retried
		// Message will contain error detail in the database.
		return pgq.MessageProcessed, fmt.Errorf("invalid payload: %v", err)
	}
	// doSomethingWithThePayload(ctx, myPayload)
	return pgq.MessageProcessed, nil
}

func ExampleConsumer() {
	db, err := sql.Open("postgres", "user=postgres password=postgres host=localhost port=5432 dbname=postgres")
	if err != nil {
		log.Fatal("Error opening database:", err)
	}
	defer db.Close()
	const queueName = "test_queue"
	c, err := pgq.NewConsumer(db, queueName, &Handler{})
	if err != nil {
		log.Fatal("Error creating consumer:", err)
	}
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal("Error running consumer:", err)
	}
}
