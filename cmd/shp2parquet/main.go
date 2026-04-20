package main

import (
	"flag"
	"fmt"
	"log"

	"mochilas/internal/convert"
)

func main() {
	in := flag.String("in", "", "input .shp path")
	out := flag.String("out", "", "output .parquet path")
	flag.Parse()
	if *in == "" || *out == "" {
		log.Fatal("usage: shp2parquet -in file.shp -out file.parquet")
	}

	s, err := convert.Convert(*in, *out)
	if err != nil {
		log.Fatalf("convert: %v", err)
	}
	fmt.Printf("wrote %d rows (%.1f MB) in %s → %s\n",
		s.Rows, float64(s.Bytes)/1024/1024, s.Duration.Round(1e6), *out)
}
