package convert

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	shp "github.com/jonas-p/go-shp"
	"github.com/parquet-go/parquet-go"
)

type Stats struct {
	Rows     int
	Bytes    int64
	Duration time.Duration
}

const batchSize = 512

func Convert(in, out string) (Stats, error) {
	start := time.Now()

	r, err := shp.Open(in)
	if err != nil {
		return Stats{}, fmt.Errorf("open shp: %w", err)
	}
	defer r.Close()

	if r.GeometryType != shp.POLYGON {
		return Stats{}, fmt.Errorf("expected polygon shapefile (5), got %d", r.GeometryType)
	}
	if len(r.Fields()) == 0 {
		base := strings.TrimSuffix(in, ".shp")
		if _, statErr := os.Stat(base + ".dbf"); statErr != nil {
			return Stats{}, fmt.Errorf("missing .dbf sidecar next to %s", in)
		}
		return Stats{}, fmt.Errorf("no attribute fields in %s.dbf", base)
	}

	schema, emitters := buildSchema(r.Fields())

	tmp := out + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return Stats{}, fmt.Errorf("create out: %w", err)
	}
	committed := false
	defer func() {
		f.Close()
		if !committed {
			os.Remove(tmp)
		}
	}()

	w := parquet.NewWriter(f, schema, parquet.Compression(&parquet.Zstd))

	rowBuf := make([]parquet.Row, 0, batchSize)
	attrBuf := make([]string, len(r.Fields()))
	var count int
	for r.Next() {
		idx, shape := r.Shape()
		poly, ok := shape.(*shp.Polygon)
		if !ok {
			return Stats{}, fmt.Errorf("record %d: not a polygon (%T)", idx, shape)
		}
		for i := range attrBuf {
			attrBuf[i] = r.ReadAttribute(idx, i)
		}
		ctx := rowCtx{
			attrs: attrBuf,
			wkb:   polygonToWKB(poly),
			bbox:  poly.BBox(),
		}
		row := make(parquet.Row, len(emitters))
		for i, em := range emitters {
			row[i] = em(&ctx).Level(0, 0, i)
		}
		rowBuf = append(rowBuf, row)
		if len(rowBuf) == cap(rowBuf) {
			if _, err := w.WriteRows(rowBuf); err != nil {
				return Stats{}, fmt.Errorf("write batch: %w", err)
			}
			rowBuf = rowBuf[:0]
		}
		count++
	}
	if len(rowBuf) > 0 {
		if _, err := w.WriteRows(rowBuf); err != nil {
			return Stats{}, fmt.Errorf("write tail: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return Stats{}, fmt.Errorf("close writer: %w", err)
	}
	if err := f.Close(); err != nil {
		return Stats{}, fmt.Errorf("close file: %w", err)
	}

	fi, err := os.Stat(tmp)
	if err != nil {
		return Stats{}, fmt.Errorf("stat tmp: %w", err)
	}
	if err := os.Rename(tmp, out); err != nil {
		return Stats{}, fmt.Errorf("rename: %w", err)
	}
	committed = true

	return Stats{Rows: count, Bytes: fi.Size(), Duration: time.Since(start)}, nil
}

type rowCtx struct {
	attrs []string
	wkb   []byte
	bbox  shp.Box
}

type emitter func(*rowCtx) parquet.Value

func buildSchema(fields []shp.Field) (*parquet.Schema, []emitter) {
	group := parquet.Group{}
	byName := make(map[string]emitter, len(fields)+5)

	for i, f := range fields {
		name := normalizeFieldName(f.Name[:])
		dbfIdx := i
		switch f.Fieldtype {
		case 'N', 'F':
			if f.Precision > 0 {
				group[name] = parquet.Leaf(parquet.DoubleType)
				byName[name] = func(ctx *rowCtx) parquet.Value {
					s := strings.TrimSpace(ctx.attrs[dbfIdx])
					if s == "" {
						return parquet.DoubleValue(0)
					}
					v, _ := strconv.ParseFloat(s, 64)
					return parquet.DoubleValue(v)
				}
			} else {
				group[name] = parquet.Int(64)
				byName[name] = func(ctx *rowCtx) parquet.Value {
					s := strings.TrimSpace(ctx.attrs[dbfIdx])
					if s == "" {
						return parquet.Int64Value(0)
					}
					v, _ := strconv.ParseInt(s, 10, 64)
					return parquet.Int64Value(v)
				}
			}
		case 'L':
			group[name] = parquet.Leaf(parquet.BooleanType)
			byName[name] = func(ctx *rowCtx) parquet.Value {
				s := strings.ToUpper(strings.TrimSpace(ctx.attrs[dbfIdx]))
				return parquet.BooleanValue(s == "T" || s == "Y")
			}
		default: // 'C', 'D', 'M', unknown → string
			group[name] = parquet.String()
			byName[name] = func(ctx *rowCtx) parquet.Value {
				return parquet.ByteArrayValue([]byte(ctx.attrs[dbfIdx]))
			}
		}
	}

	group["geometry"] = parquet.Leaf(parquet.ByteArrayType)
	byName["geometry"] = func(ctx *rowCtx) parquet.Value { return parquet.ByteArrayValue(ctx.wkb) }
	group["bbox_minx"] = parquet.Leaf(parquet.DoubleType)
	byName["bbox_minx"] = func(ctx *rowCtx) parquet.Value { return parquet.DoubleValue(ctx.bbox.MinX) }
	group["bbox_miny"] = parquet.Leaf(parquet.DoubleType)
	byName["bbox_miny"] = func(ctx *rowCtx) parquet.Value { return parquet.DoubleValue(ctx.bbox.MinY) }
	group["bbox_maxx"] = parquet.Leaf(parquet.DoubleType)
	byName["bbox_maxx"] = func(ctx *rowCtx) parquet.Value { return parquet.DoubleValue(ctx.bbox.MaxX) }
	group["bbox_maxy"] = parquet.Leaf(parquet.DoubleType)
	byName["bbox_maxy"] = func(ctx *rowCtx) parquet.Value { return parquet.DoubleValue(ctx.bbox.MaxY) }

	schema := parquet.NewSchema("shp_row", group)

	cols := schema.Columns()
	emitters := make([]emitter, len(cols))
	for i, path := range cols {
		emitters[i] = byName[path[0]]
	}
	return schema, emitters
}

func normalizeFieldName(raw []byte) string {
	if i := bytes.IndexByte(raw, 0); i >= 0 {
		raw = raw[:i]
	}
	return strings.ToLower(strings.TrimSpace(string(raw)))
}

const (
	wkbLittleEndian = 1
	wkbPolygon      = 3
	wkbMultiPolygon = 6
)

func polygonToWKB(p *shp.Polygon) []byte {
	var buf bytes.Buffer
	buf.WriteByte(wkbLittleEndian)
	writeUint32(&buf, wkbMultiPolygon)
	writeUint32(&buf, uint32(p.NumParts))

	parts := append([]int32{}, p.Parts...)
	parts = append(parts, p.NumPoints)

	for i := int32(0); i < p.NumParts; i++ {
		start, end := parts[i], parts[i+1]
		buf.WriteByte(wkbLittleEndian)
		writeUint32(&buf, wkbPolygon)
		writeUint32(&buf, 1)
		writeUint32(&buf, uint32(end-start))
		for _, pt := range p.Points[start:end] {
			writeFloat64(&buf, pt.X)
			writeFloat64(&buf, pt.Y)
		}
	}
	return buf.Bytes()
}

func writeUint32(w *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.Write(b[:])
}

func writeFloat64(w *bytes.Buffer, v float64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	w.Write(b[:])
}
