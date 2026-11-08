.DEFAULT_GOAL := check

.PHONY: check test lint fmt bench

check: lint test

test:
	uv run python -m unittest discover -s tests -v

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run ty check

fmt:
	uv run ruff check --fix .
	uv run ruff format .

bench:
	uv run python benchmarks/run.py --output benchmarks/results.json
