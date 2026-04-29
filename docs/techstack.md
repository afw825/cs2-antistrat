Tech Stack Decision Records (TSDR)
This document records the technology choices for the CS2 Anti-Strat project. These decisions prioritize rapid iteration, maintainability, and reproducible analysis within semester constraints.

TSDR 001: Python as Primary Language
Status: Accepted

Context: The project requires data-heavy parsing, transformation, and analysis with a strong testing story and low setup friction for contributors.

Decision: Use Python 3.11+ as the primary implementation language.

Alternatives Considered:

Node.js/TypeScript: Rejected because the parser and analysis ecosystem for this specific workflow is less direct than Python's data stack.
C++/Rust: Rejected due to longer development time and higher complexity for class-scope delivery.

Consequences:

Positive: Fast development velocity; rich ecosystem for parsing/data analysis; strong readability for team collaboration.
Negative: Lower raw runtime performance than compiled alternatives for some workloads.

TSDR 002: Streamlit for UI Layer
Status: Accepted

Context: Analysts need a simple, interactive interface to upload demos, trigger parsing, and apply filters without building a full SPA frontend.

Decision: Use Streamlit as the primary user interface framework.

Alternatives Considered:

FastAPI + React: Rejected due to higher frontend/backend coordination overhead for the current scope.
CLI-Only Interface: Rejected because non-technical users benefit from an interactive visual workflow.

Consequences:

Positive: Rapid UI delivery; low boilerplate; easy integration with Python data tooling.
Negative: Less control over advanced frontend UX patterns compared to custom web stacks.

TSDR 003: SQLAlchemy + SQLite for Persistence
Status: Accepted

Context: Parsed telemetry must be queryable and reusable across analysis sessions, while keeping local development simple.

Decision: Use SQLAlchemy ORM for persistence access and SQLite for the current database backend.

Alternatives Considered:

Raw SQL Only: Rejected because it increases repetitive query boilerplate and maintenance burden.
PostgreSQL for All Environments: Rejected for now due to unnecessary infrastructure setup for semester scope.

Consequences:

Positive: Durable, queryable telemetry store; explicit relational modeling; minimal environment setup.
Negative: SQLite concurrency and scaling limits compared with server databases.

TSDR 004: Poetry + Pytest + Ruff + Pre-Commit + GitHub Actions Quality Gates
Status: Accepted

Context: The project needs consistent dependency management, reliable testing, and automated code quality checks before merge.

Decision: Use Poetry for dependencies, Pytest for testing, Ruff for lint/format, Pre-Commit for local hooks, and GitHub Actions for CI enforcement.

Alternatives Considered:

pip + requirements.txt without lock management: Rejected because reproducibility and dependency consistency are weaker.
Manual quality checks only: Rejected because inconsistent local workflows increase merge risk.

Consequences:

Positive: Reproducible environments; faster defect detection; consistent coding standards across contributors.
Negative: Slightly longer setup time and CI runtime due to enforced quality gates.
