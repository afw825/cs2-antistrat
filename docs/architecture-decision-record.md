# Architecture Decision Record (ADR)

## ADR-001: Layered Data-Pipeline Architecture for CS2 Demo Analysis

- Status: Accepted
- Date: 2026-04-01
- Context:
  - The project must parse large CS2 `.dem` files and support repeatable analysis.
  - The class rubric requires maintainable architecture and clear testability boundaries.
- Decision:
  - Use a layered architecture:
    - API layer: user-facing interactions and endpoints.
    - Ingestion layer: parsing, filtering, and transformation of demo telemetry.
    - Persistence layer: relational schema via SQLAlchemy.
    - Visualization layer: map/radar plotting.
  - Persist transformed data to the database instead of plotting directly from raw parser output.
- Consequences:
  - Pros: clear separation of concerns, easier testing, reproducible analysis, simpler future API extensions.
  - Cons: added schema complexity and migration concerns.
- Alternatives Considered:
  - Single script flow with in-memory data only (rejected due to poor maintainability and no durable query layer).
  - No ORM and raw SQL only (rejected due to reduced developer ergonomics in class setting).
