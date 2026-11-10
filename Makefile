.DEFAULT_GOAL := check

.PHONY: check test lint fmt bench

BENCH_ARGS ?=

check: lint test

test:
	uv run python -X dev -W error -m unittest discover -s tests -v

bench:
	uv run python benchmarks/bench.py tcp $(BENCH_ARGS)
	
lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run ty check
	uv run pyright

fmt:
	uv run ruff check --fix .
	uv run ruff format .
