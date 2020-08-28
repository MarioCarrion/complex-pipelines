package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func main() {
	size, timeout := parseFlags()

	//-

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Duration(timeout)*time.Minute))
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Connecting to db %s", err)
	}

	//-

	g, ctx := errgroup.WithContext(ctx)

	var recordsC <-chan name

	wait := make(chan struct{}, 1)

	g.Go(func() error {
		var (
			err  error
			errC <-chan error
		)

		recordsC, errC, err = produce(ctx)
		if err != nil {
			close(wait)
			return err
		}

		close(wait)

		return <-errC
	})

	<-wait

	g.Go(func() error {
		errC := consume(ctx, conn, size, recordsC)
		return <-errC
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("processing error %s", err)
	}

	fmt.Println("Done")
}

//-

func produce(ctx context.Context) (<-chan name, <-chan error, error) {
	req, err := http.NewRequest(http.MethodGet, "https://datasets.imdbws.com/name.basics.tsv.gz", nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "instantiating request")
	}

	client := &http.Client{
		Timeout: 10 * time.Minute,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "doing request")
	}

	outC := make(chan name)
	errC := make(chan error, 1)

	go func() {
		defer func() {
			close(outC)
			close(errC)

			resp.Body.Close()
		}()

		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			errC <- errors.Wrap(err, "instantiating gzip")
			return
		}
		defer gr.Close()

		cr := bufio.NewReader(gr)

		skipHeader := true

		for {
			line, err := cr.ReadString('\n')
			if err == io.EOF {
				return
			}

			if skipHeader {
				skipHeader = false
				continue
			}

			if err != nil {
				errC <- errors.Wrap(err, "reading gzip")
				return
			}

			record := strings.Split(strings.Trim(line, "\n"), "\t")
			n := name{
				NConst:             record[0],
				PrimaryName:        record[1],
				BirthYear:          record[2],
				DeathYear:          record[3],
				PrimaryProfessions: strings.Split(record[4], ","),
				KnownForTitles:     strings.Split(record[5], ","),
			}

			select {
			case outC <- n:
			case <-ctx.Done():
				errC <- ctx.Err()
				return
			}
		}
	}()

	return outC, errC, nil
}

//-

func consume(ctx context.Context, conn *pgx.Conn, size int64, recordsC <-chan name) <-chan error {
	outErrC := make(chan error, 1)

	go func() {
		defer close(outErrC)

		batcher := newBatcher(conn, size)
		errC := batcher.Copy(ctx, recordsC)

		for {
			select {
			case err := <-errC:
				outErrC <- err
				return
			case <-ctx.Done():
				outErrC <- ctx.Err()
				return
			}
		}
	}()

	return outErrC
}

//-

type name struct {
	NConst             string
	PrimaryName        string
	BirthYear          string
	DeathYear          string
	PrimaryProfessions []string
	KnownForTitles     []string
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
		namesC: namesC,
		errorC: errorC,
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

type batcher struct {
	conn *pgx.Conn
	size int64
}

func newBatcher(conn *pgx.Conn, size int64) *batcher {
	return &batcher{
		conn: conn,
		size: size,
	}
}

func (b *batcher) Copy(ctx context.Context, namesC <-chan name) <-chan error {
	outErrC := make(chan error)
	var mutex sync.Mutex
	var copyFromErr error

	copyFrom := func(batchNamesC <-chan name, batchErrC <-chan error) <-chan error {
		cpOutErrorC := make(chan error)

		go func() {
			defer close(cpOutErrorC)

			copier := newCopyFromSource(batchNamesC, batchErrC)

			_, err := b.conn.CopyFrom(ctx,
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

			if err != nil {
				mutex.Lock()
				copyFromErr = err
				mutex.Unlock()
			}
		}()

		return cpOutErrorC
	}

	go func() {
		batchErrC := make(chan error)
		batchNameC := make(chan name)

		cpOutErrorC := copyFrom(batchNameC, batchErrC)

		defer func() {
			close(batchErrC)
			close(batchNameC)
			close(outErrC)
		}()

		var index int64

		for {
			select {
			case n, open := <-namesC:
				if !open {
					return
				}

				mutex.Lock()
				if copyFromErr != nil {
					namesC = nil
					mutex.Unlock()
					outErrC <- copyFromErr
					return
				}
				mutex.Unlock()

				batchNameC <- n

				index++

				if index == b.size {
					close(batchErrC)
					close(batchNameC)

					if err := <-cpOutErrorC; err != nil {
						outErrC <- err
						return
					}

					batchErrC = make(chan error)
					batchNameC = make(chan name)

					cpOutErrorC = copyFrom(batchNameC, batchErrC)
					index = 0
				}
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					batchErrC <- err
					outErrC <- err
					return
				}
			}
		}
	}()

	return outErrC
}

//-

func parseFlags() (int64, int64) {
	var (
		size    int64
		timeout int64
	)

	flag.Int64Var(&size, "size", 100_000, "batch size")
	flag.Int64Var(&timeout, "timeout", 20, "timeout in minutes")
	flag.Parse()

	if size <= 0 {
		size = 100_000
	}

	if timeout <= 0 {
		timeout = 20
	}

	return size, timeout
}
