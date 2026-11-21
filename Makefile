IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
TARGET_CPU ?= cascadelake
DEV   ?= dev
FIXTURES ?= $(if $(wildcard fixtures),fixtures,starter-duckdb/starter-duckdb/fixtures)
ANSWER ?= dev-answer/dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev

build:
	docker buildx build --platform $(PLATFORM) --build-arg TARGET_CPU=$(TARGET_CPU) --load -t $(IMAGE) .

fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

check-dev:
	python3 scripts/compare.py out $(ANSWER)
