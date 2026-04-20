package main

import (
	"fmt"
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: pqschema <file.parquet>")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows=%d columns:\n", pf.NumRows())
	for _, c := range pf.Schema().Columns() {
		fmt.Printf("  %v\n", c)
	}
}
