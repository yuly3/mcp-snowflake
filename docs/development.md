# Development

## Development Environment Setup

```bash
uv sync --all-groups --all-packages
```

## Code Formatting

```bash
uv run ruff format .
uv run ruff check --fix .
```

## Code Testing

```bash
uv run pytest --doctest-modules .
```
