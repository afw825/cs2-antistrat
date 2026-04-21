# Tech Stack Decision Record

## TSDR-001: Python + Streamlit + SQLAlchemy + SQLite

- Status: Accepted
- Date: 2026-04-01

## Chosen Stack

- Language: Python 3.11+
- Dependency management: Poetry
- UI: Streamlit
- Parsing: demoparser2
- ORM/DB access: SQLAlchemy
- Database (dev/test): SQLite
- Data processing: pandas, numpy
- Visualization: matplotlib, Pillow
- Testing: pytest
- Quality gates: ruff + pre-commit + GitHub Actions

## Why This Stack

- Fast iteration for data-heavy workflows in a class timeline.
- Rich Python ecosystem for parsing and analysis.
- SQLite keeps setup friction low while preserving relational modeling.
- SQLAlchemy keeps DB interactions explicit and testable.

## Alternatives Considered

- FastAPI + React frontend:
  - Strong for production APIs, but too much overhead for class scope.
- PostgreSQL in all environments:
  - Better for scale, but unnecessary setup complexity for current requirements.

## Operational Decision

- Because SQLite is used, Dockerized dev/test DB containers are not required.
- If later migrated away from SQLite, dev/test DBs should be dockerized and production DB hosted on Render or equivalent.
