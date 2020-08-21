// Example demostrating how cancelation works when using `errgroup`
// Increasing the value of the deadline to something larger than 400
// milliseconds will never prevent this pipelie succeed.
//
// https://www.mariocarrion.com/2020/08/19/go-implementing-complex-pipelines-part-4.html
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(200*time.Millisecond)) // Replace "200" with anything larger than 400
	defer cancel()

	messages := make(chan string)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(messages)

		for _, msg := range []string{"a", "b", "c", "d"} {
			select {
			case messages <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	g.Go(func() error {
		for {
			select {
			case msg, open := <-messages:
				if !open {
					return nil
				}
				fmt.Println(msg)
			case <-ctx.Done():
				return ctx.Err()
			}

			time.Sleep(100 * time.Millisecond)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		log.Fatalln("wait", err)
	}
}
