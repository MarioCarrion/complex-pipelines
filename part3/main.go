package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	f, err := os.Open(parseFlags())
	if err != nil {
		log.Fatalf("Opening file %s", err)
	}
	defer f.Close()

	br := bufio.NewReader(f)

	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			return
		}

		if err != nil {
			log.Fatalf("Reading TSV %s", err)
		}

		fmt.Printf("%#v\n", strings.Split(strings.Trim(line, "\n"), "\t"))
	}
}

//-

func parseFlags() string {
	var file string

	flag.StringVar(&file, "file", "name.basics.tsv", "tab separated values file")
	flag.Parse()

	return file
}
