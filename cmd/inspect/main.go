package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	shp "github.com/jonas-p/go-shp"
)

func fieldName(f shp.Field) string {
	b := f.Name[:]
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: inspect <path.shp>")
	}
	r, err := shp.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	fmt.Println("ShapeType:", r.GeometryType)
	fmt.Println("Fields:")
	for i, f := range r.Fields() {
		fmt.Printf("  [%d] name=%q type=%c len=%d dec=%d\n",
			i, fieldName(f), f.Fieldtype, f.Size, f.Precision)
	}

	n := 0
	for r.Next() {
		if n == 0 {
			idx, shape := r.Shape()
			bbox := shape.BBox()
			fmt.Printf("First record idx=%d bbox=%+v\n", idx, bbox)
			for i, f := range r.Fields() {
				fmt.Printf("  %s=%q\n", fieldName(f), r.ReadAttribute(idx, i))
			}
		}
		n++
	}
	fmt.Println("Total records:", n)
}
