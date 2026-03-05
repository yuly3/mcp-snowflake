# AGENTS.md

## Quality Gate

The primary quality gate command that runs all checks:

```bash
uv run ruff format .
uv run ruff check . --fix --unsafe-fixes
uv run pyright . --level warning
uv run pytest tests
uv run pytest --doctest-modules src packages
```
