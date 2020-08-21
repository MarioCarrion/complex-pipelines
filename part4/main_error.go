// Example demostrating how cancelation works when using `errgroup`
// Increasing the value of the deadline to something larger than 400
// milliseconds will never prevent this pipelie succeed.
//
// https://www.mariocarrion.com/2020/08/19/go-implementing-complex-pipelines-part-4.html
package main

import (
	"context"
	"errors"
	"log"

	"golang.org/x/sync/errgroup"
)

func main() {
	messages := make(chan string)

	g, ctx := errgroup.WithContext(context.Background())

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

				if msg == "c" {
					return errors.New("I don't like c")
				}

			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		log.Fatalln("wait", err)
	}
}
