# pgcmp

A PostgreSQL schema comparison tool.

## Installation

```bash
uv sync
```

## Usage

Compare two PostgreSQL databases:

```bash
uv run python main.py <left_db> <right_db>
```

The left database is the "source" (what you want), and the right database is the "target" (what you have).

### Examples

```bash
# Compare two databases
uv run python main.py "postgresql://user:pass@host1:5432/db1" "postgresql://user:pass@host2:5432/db2"

# Output as XML
uv run python main.py --xml "postgres://localhost/db1" "postgres://localhost/db2"

# Compare row counts only
uv run python main.py --row-counts "postgres://localhost/db1" "postgres://localhost/db2"
```

### Options

| Option | Description |
|--------|-------------|
| `--xml` | Output results in XML format |
| `--row-counts` | Compare row counts instead of schema |

## What It Compares

- Schemas
- Tables
- Columns (type, nullability, defaults)
- Views
- Materialized views
- Indexes
- Constraints
- Triggers
- Functions

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No differences found |
| 1 | Connection error |
| 2 | Differences found |
