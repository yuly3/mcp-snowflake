---
applyTo: "**"
---

# Project general coding standards

- Python in this project is managed by `uv`. For all Python-related tasks, you must use the `uv` command (e.g., installing packages, running scripts, managing the environment) instead of `pip` or other Python tools.


## Task Completion Documentation

- When you complete a task, create a markdown file in the `docs/dev-notes/{YYYY-MM-DD}/` directory. The filename should be the task title (e.g., `implement-describe-table-tool.md`). In this file, write the user's prompt and a summary of the completed task.

## Example commands

- Install a development dependency:
  ```sh
  uv add --dev {lib}
  ```
- Run pytest:
  ```sh
  uv run pytest {path}
  ```
