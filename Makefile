SHP      ?= shp/BR_Municipios_2025.shp
OUT      ?= shp/BR_Municipios_2025.parquet

IN_DIR   ?= shp
OUT_DIR  ?= out

WORKERS  ?= 4
BIN      ?= bin/shp2parquet
POOL_BIN ?= bin/shp2parquet-pool

.PHONY: build build-pool convert inspect run pool pool-build clean

build:
	@mkdir -p bin
	go build -o $(BIN) ./cmd/shp2parquet

build-pool:
	@mkdir -p bin
	go build -o $(POOL_BIN) ./cmd/shp2parquet-pool

convert: build
	$(BIN) -in $(SHP) -out $(OUT)

run:
	go run ./cmd/shp2parquet -in $(SHP) -out $(OUT)

pool:
	go run ./cmd/shp2parquet-pool -in-dir $(IN_DIR) -out-dir $(OUT_DIR) -workers $(WORKERS)

pool-build: build-pool
	$(POOL_BIN) -in-dir $(IN_DIR) -out-dir $(OUT_DIR) -workers $(WORKERS)

inspect:
	go run ./cmd/inspect $(SHP)

schema:
	go run ./cmd/pqschema $(OUT)

clean:
	rm -rf bin $(OUT) $(OUT_DIR)
