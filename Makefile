BINARY       := target/release/kvdist
DOCKER_IMAGE := kvdist

.PHONY: all build run test lint clean docker-build docker-run cluster cluster-down

all: build

build:
	cargo build --release

run: build
	./$(BINARY) --role root --data-dir ./data/root

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

clean:
	cargo clean
	rm -rf ./data

docker-build:
	docker build -t $(DOCKER_IMAGE) .

docker-run:
	docker run --rm -p 8080:8080 -p 7070:7070 -v kvdist-data:/data $(DOCKER_IMAGE)

# Root -> replica -> proxy, see docker-compose.yml
cluster:
	docker compose up --build

cluster-down:
	docker compose down -v
