.PHONY: test lint

test:
	uv run python -m unittest discover -s tests/codec -v
