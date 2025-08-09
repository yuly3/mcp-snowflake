---
applyTo: "**"
---

# About this project

This repository implements a Snowflake MCP server.

## Project general coding standards

- Python in this project is managed by `uv`. For all Python-related tasks, you must use the `uv` command (e.g., installing packages, running scripts, managing the environment) instead of `pip` or other Python tools.


### Development Documentation Process

- **Planning Phase**: When starting a task, first create an implementation plan and discuss it with the user. Once the user agrees with the plan, create a markdown file in the `docs/dev-notes/{YYYY-MM-DD}/` directory. The filename should be the task title (e.g., `implement-describe-table-tool.md`). In this file, write the user's prompt and the agreed implementation plan.

- **Completion Phase**: After completing the task, update the same markdown file to include a summary of the completed work, any changes made during implementation, and final results.

### Example commands

- Install a development dependency:
  ```sh
  uv add --dev {lib}
  ```
- Run pytest:
  ```sh
  uv run pytest {path}
  ```
- Run doctest:
  ```sh
  uv run pytest --doctest-modules {path}
  ```
- Run ruff:
  ```sh
  uv run ruff check --fix --unsafe-fixes {path}
  uv run ruff format {path}
  ```
