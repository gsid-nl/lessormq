// Publish three messages and work them off with the LessorMQ worker.
// LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); go get github.com/gsid-nl/lessormq-go.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	lessormq "github.com/gsid-nl/lessormq-go"
)

func getenv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	client := lessormq.New(getenv("LESSORMQ_URL", "http://127.0.0.1:18080"), os.Getenv("LESSORMQ_KEY"))
	queue := getenv("LESSORMQ_QUEUE", "examples")

	for _, body := range []map[string]any{{"n": 1}, {"n": 2, "fail_once": true}, {"n": 3}} {
		published, err := client.Publish(ctx, queue, body, nil)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("published", published.ID)
	}

	var handled atomic.Int32
	worker := lessormq.NewWorker(client, queue, func(ctx context.Context, job *lessormq.Job) error {
		var body struct {
			N        int  `json:"n"`
			FailOnce bool `json:"fail_once"`
		}
		if err := job.Message.Decode(&body); err != nil {
			return lessormq.Permanent(err)
		}
		if body.FailOnce && job.Message.Attempt == 1 {
			fmt.Printf("n=%d attempt=%d failed\n", body.N, job.Message.Attempt)
			return errors.New("temporarily unavailable")
		}
		fmt.Printf("n=%d attempt=%d handled\n", body.N, job.Message.Attempt)
		if handled.Add(1) == 3 {
			cancel()
		}
		return nil
	}, lessormq.WorkerOptions{Consumer: "go-example", WaitSeconds: 5})

	_ = worker.Run(ctx)
	fmt.Printf("done: %d handled\n", handled.Load())
	if handled.Load() != 3 {
		os.Exit(1)
	}
}
