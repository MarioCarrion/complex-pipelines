package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/bxcodec/faker/v3"
	"github.com/jackc/pgx/v4"
)

func main() {
	amount, size := parseFlags()

	setUpFaker()

	//-

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Connecting to db %s", err)
	}

	outC := make(chan name)
	errorC := make(chan error)

	waitC := make(chan struct{})

	go func() {
		defer func() {
			close(outC)
			close(errorC)
		}()

		for amount > 0 {
			amount--

			n := name{}
			if err := faker.FakeData(&n); err != nil {
				errorC <- err
				return
			}

			outC <- n
		}
	}()

	go func() {
		defer close(waitC)

		batcher := newBatcher(conn, size)
		errC := batcher.Copy(context.Background(), outC)
		if err := <-errC; err != nil {
			fmt.Printf("Error copying %s\n", err)
		}
	}()

	<-waitC

	fmt.Println("Done")
}

//-

type name struct {
	NConst             string
	PrimaryName        string   `faker:"name"`
	BirthYear          string   `faker:"yearString"`
	DeathYear          string   `faker:"yearString"`
	PrimaryProfessions []string `faker:"len=5"`
	KnownForTitles     []string `faker:"len=7"`
}

func (n name) Values() []interface{} {
	v := make([]interface{}, 6)

	v[0] = n.NConst
	v[1] = n.PrimaryName
	v[2] = n.BirthYear
	v[3] = n.DeathYear
	v[4] = n.PrimaryProfessions
	v[5] = n.KnownForTitles

	return v
}

//-

type copyFromSource struct {
	errorC  <-chan error
	namesC  <-chan name
	err     error
	closed  bool
	current name
}

func newCopyFromSource(namesC <-chan name, errorC <-chan error) *copyFromSource {
	return &copyFromSource{
		errorC: errorC,
		namesC: namesC,
	}
}

func (c *copyFromSource) Err() error {
	return c.err
}

func (c *copyFromSource) Next() bool {
	if c.closed {
		return false
	}

	var open bool

	select {
	case c.current, open = <-c.namesC:
	case c.err = <-c.errorC:
	}

	if !open {
		c.closed = true
		return false
	}

	if c.err != nil {
		return false
	}

	return true
}

func (c *copyFromSource) Values() ([]interface{}, error) {
	return c.current.Values(), nil
}

//-

type copyFromSourceMediator struct {
	namesC chan name
	errorC chan error
	copier *copyFromSource
}

func newCopyFromSourceMediator(conn *pgx.Conn) (*copyFromSourceMediator, <-chan error) {
	errorC := make(chan error)
	namesC := make(chan name)

	copier := newCopyFromSource(namesC, errorC)

	res := copyFromSourceMediator{
		namesC: namesC,
		errorC: errorC,
		copier: copier,
	}

	outErrorC := make(chan error)

	go func() {
		defer close(outErrorC)

		_, err := conn.CopyFrom(context.Background(),
			pgx.Identifier{"names"},
			[]string{
				"nconst",
				"primary_name",
				"birth_year",
				"death_year",
				"primary_professions",
				"known_for_titles",
			},
			copier)

		outErrorC <- err
	}()

	return &res, outErrorC
}

func (c *copyFromSourceMediator) Batch(n name) {
	c.namesC <- n
}

func (c *copyFromSourceMediator) Err(err error) {
	c.errorC <- err
}

func (c *copyFromSourceMediator) CopyAll() {
	close(c.namesC)
	close(c.errorC)
}

//-

type batcher struct {
	conn *pgx.Conn
	size int
}

func newBatcher(conn *pgx.Conn, size int) *batcher {
	return &batcher{
		conn: conn,
		size: size,
	}
}

func (b *batcher) Copy(ctx context.Context, namesC <-chan name) <-chan error {
	outErrC := make(chan error)

	go func() {
		mediator, errorC := newCopyFromSourceMediator(b.conn)

		copyAll := func(m *copyFromSourceMediator, c <-chan error) error {
			m.CopyAll()
			return <-c
		}

		defer func() {
			if err := copyAll(mediator, errorC); err != nil {
				outErrC <- err
			}

			close(outErrC)
		}()

		var index int

		for {
			select {
			case name, open := <-namesC:
				if !open {
					return
				}

				mediator.Batch(name)
				index++

				if index == b.size {
					if err := copyAll(mediator, errorC); err != nil {
						outErrC <- err
					}

					mediator, errorC = newCopyFromSourceMediator(b.conn)
					index = 0
				}
			case err := <-errorC:
				outErrC <- err
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					mediator.Err(err)
					outErrC <- err
				}
			}
		}
	}()

	return outErrC
}

//-

func parseFlags() (int, int) {
	var amount, size int

	flag.IntVar(&size, "size", 100_000, "batch size")
	flag.IntVar(&amount, "amount", 500_000, "amount of fakes to generate")
	flag.Parse()

	if size <= 0 {
		size = 100_000
	}
	if amount <= 0 {
		amount = 500_000
	}

	return amount, size
}

func setUpFaker() {
	_ = faker.AddProvider("name", func(_ reflect.Value) (interface{}, error) {
		return faker.Name(), nil
	})

	_ = faker.AddProvider("yearString", func(_ reflect.Value) (interface{}, error) {
		return faker.YearString(), nil
	})
}
