# PGQ - go queues using postgres

[![GoDoc](https://pkg.go.dev/badge/go.dataddo.com/pgq)](https://pkg.go.dev/go.dataddo.com/pgq)
[![GoReportCard](https://goreportcard.com/badge/go.dataddo.com/pgq)](https://goreportcard.com/report/go.dataddo.com/pgq)

PGQ is a Go package that provides a queue mechanism for your Go applications built on top of the postgres database. 
It enables developers to implement efficient and reliable message queues for their microservices architecture using the familiar postgres infrastructure.

PGQ internally uses the classic `UPDATE` + `SELECT ... FOR UPDATE` postgres statement which creates the transactional lock for the selected rows in the postgres table and enables the table to behave like the queue.
The Select statement is using the `SKIP LOCKED` clause which enables the consumer to fetch the messages in the queue in the order they were created, and doesn't get stuck on the locked rows.
It is intended to replace special message broker in environments where you already use postgres, and you want clean, simple and straightforward communication among your services.

## Features
- Postgres-backed: Leverages the power of PostgreSQL to store and manage queues.
- Reliable: Guarantees message persistence and delivery, even in the face of failures.
- Efficient: Optimized for high throughput and low-latency message processing.
- Transactional: Supports transactional message handling, ensuring consistency.
- Simple API: Provides a clean and easy-to-use API for interacting with the queue.

## Installation
To install PGQ, use the go get command:
```
go get go.dataddo.com/pgq@latest
```

## Setup
In order to make pgq functional, there must exist the postgres table with all the fields the pgq requires.
You can create the table query on your own, or you can use the query generator for that. 
You usually run the create table command just once during the setup of your application.

```go
package main

import (
	"fmt"
	"go.dataddo.com/pgq/x/schema"
)

func main() {
	queueName := "my_queue"

	// create string contains the "CREATE TABLE my_queue ..." which you may use for table creation 
	create := schema.GenerateCreateTableQuery(queueName)
	fmt.Println(create)

	// drop string contains the "DROP TABLE my_queue ..." which you may use for cleaning hwn you no longer need the queue 
	drop := schema.GenerateCreateTableQuery(queueName)
	fmt.Println(drop)
}

```

## Usage
```
import "go.dataddo.com/pgq"

// Create a new consumer/subscriber
// To be added

// Create a new publisher
// To be added

// Publish message
// To be added
```

For more detailed usage examples and API documentation, please refer to the GoDoc page.


## Message

The message is the essential structure for communication between services using pgq. The message struct matches the postgres table. You can modify the table structure on your own by adding extra columns, but pgq depends on these mandatory fields"
- `id`: The unique ID of the message in the db
- `created_at`: The timestamp when the record in db was created (message received to the queue)
- `payload`: Your custom message content in JSON format
- `metadata`: Your optional custom metadata about the message in JSON format so your `payload` remains clean. This is the good place where to put information like the `publisher` app name, publish timestamp, payload schema version, customer related information etc.
- `started_at`: Timestamp when the consumer started to process the message
- `locked_until`: Timestamp stating until when the consumer wants to have the lock applied. If this field is set, no other consumer will process this message.
- `processed_at`: Timestamp when the message was processed (either success or failure)
- `error_detail`: The reason why the processing of the message failed provided by the consumer. Default `NULL` means no error.
- `consumed_count`: The incremented integer keeping how many times the messages was tried to be consumed. Preventing the everlasting consumption of message which causes the OOM of consumers or other defects.

## Handy queue sql queries

```
// messages waiting in the queue to be fetched by consumer (good candidate for the queue length metric)
select * from table_name where processed_at is null and locked_until is null;

// messages being processed at the moment (good candidate for the messages WIP metric)
select * from table_name where processed_at is null and locked_until is not null;

// messages which failed when being processed
select * from table_name where processed_at is not null and error_detail is not null;

// messages created in last 1 day which have not been processed yet
select * from table_name where processed_at is null and created_at > NOW() - INTERVAL '1 DAY';

// messages causing unexpected failures of consumers (OOM ususally) 
select * from table_name where consumed_count > 1;

// top 10 slowest processed messages
select id, processed_at - started_at as duration  from extractor_input where processed_at is not null and started_at is not null order by duration desc limit 10;
```

## Optimizing performance

In order to get the most out of your postgres, you should invest some time configuring pgq to optimize it's performance.
- __create indices__ for the fields the pgq uses
- use postgres __tables partitioning__ (pg_partman) to speed up queries

## Use Cases for Queues in Microservice Architecture
- Asynchronous Communication: Queues enable decoupled communication between microservices by allowing them to exchange messages asynchronously. This promotes scalability, fault tolerance, and flexibility in building distributed systems.
- Event-Driven Architecture: Queues serve as a backbone for event-driven architectures, where events are produced by services and consumed by interested consumers. This pattern enables loose coupling and real-time processing of events, facilitating reactive and responsive systems.
- Load Balancing: Queues can distribute the workload across multiple instances of a microservice. They ensure fair and efficient processing of tasks by allowing multiple workers to consume messages from the queue in a load-balanced manner.
- Task Scheduling: Queues can be used for scheduling and executing background tasks or long-running processes asynchronously. This approach helps manage resource-intensive tasks without blocking the main execution flow.

## Other Queueing Tools for Microservice Architecture
While PGQ offers a Postgres-based queueing solution, there are several other popular tools available for implementing message queues in microservice architectures:

__RabbitMQ__: A feature-rich, open-source message broker that supports multiple messaging patterns, such as publish/subscribe and request/reply. It provides robust message queuing, routing, and delivery guarantees.

__Kafka__: A distributed streaming platform that is highly scalable and fault-tolerant. Kafka is designed for handling high-throughput, real-time data streams and provides strong durability and fault tolerance guarantees.

__Amazon Simple Queue Service (SQS)__: A fully managed message queuing service provided by AWS. SQS offers reliable and scalable queues with automatic scaling, high availability, and durability.

__Google Cloud Pub/Sub__: A messaging service from Google Cloud that provides scalable and reliable message queuing and delivery. It offers features like event-driven processing, push and pull subscriptions, and topic-based messaging.

__Microsoft Azure Service Bus__: A cloud-based messaging service on Microsoft Azure that enables reliable communication between services and applications. It supports various messaging patterns and provides advanced features like message sessions and dead-lettering.

Choose a queueing tool based on your specific requirements, such as scalability, fault tolerance, delivery guarantees, integration capabilities, and cloud provider preferences.

## When to pick PGQ?

Even though the technologies listed above are great for complex messaging including the robust routing configuration, sometimes you do not need it for your simple use cases.

If you need just the basic routing, distribute the payload fairly to protect your services from overloading and want to use technology which is already in your tech stack (postgres), the __pgq__ is the right choice. No need to bring the new technology to your stack when you can be satisfied with postgres.

Write consumers and publishers in various languages with the simple idea behind - use postgre table as a queue.

While using pgq you have a superb observability of what is going on in the queue. You can easily see the messages and their content which are currently being processed. You can see how long the processing takes, if it succeeded or and why it failed. As the queue remebers the already processed jobs too, you have out of the box the historical statistics and can view it effortlessly by using regular SQL queries.


