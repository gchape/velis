.PHONY: test lint fmt

test:
	uv run python -m unittest discover -s tests -v

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run ty check