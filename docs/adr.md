Architecture Decision Records (ADR)
This document records the architectural design choices for the CS2 Anti-Strat project. These decisions prioritize repeatable tactical analysis, maintainability, and reliable ingestion over infrastructure complexity.

ADR 001: Layered Modular Monolith Architecture
Status: Accepted

Context: The system must parse CS2 demo files, transform high-volume telemetry, and present filtered tactical visualizations. Class scope favors a single deployable application with strong internal structure rather than distributed services.

Decision: We will implement a layered modular monolith.

Layered: The application is partitioned into API, Ingestion, Persistence, and Visualization layers with clear boundaries.
Modular: Domain logic is separated by module so parser, database, and UI changes can be made independently.

Alternatives Considered:

Microservices: Rejected as service orchestration and inter-service communication add unnecessary complexity for a semester project.
Single Script Pipeline: Rejected due to tight coupling between parsing, storage, and rendering, which reduces testability.

Consequences:

Positive: Clear separation of concerns; easier unit and integration testing; simpler future feature additions.
Negative: More upfront code organization and interface management than a quick prototype.

ADR 002: Persistent Relational Telemetry Store
Status: Accepted

Context: Analysts need to re-run filters and visualizations without re-parsing the same demos every time. A durable query layer is required for repeatable analysis sessions.

Decision: Persist transformed telemetry into a normalized relational schema using SQLAlchemy with SQLite in the current environment.

Alternatives Considered:

In-Memory Only Processing: Rejected because every analysis operation would require full reparsing and recomputation.
Raw SQL Without ORM: Rejected to avoid boilerplate-heavy data access and reduce developer friction.

Consequences:

Positive: Fast re-querying of parsed data; reproducible analysis; explicit data model for matches, rounds, players, and ticks.
Negative: Schema evolution and migration concerns; added complexity compared to transient in-memory dataframes.

ADR 003: Unidirectional Batch Demo Processing
Status: Accepted

Context: Demo files are uploaded as completed artifacts, not event streams. Opening-window tactical analysis requires deterministic extraction and transformation from static input files.

Decision: Use a batch processing model where data flows one way: demo file -> parser/filter -> persistence -> visualization. No writes occur back to source demo files.

Alternatives Considered:

Event-Driven Streaming Pipeline: Rejected because `.dem` analysis is file-based and does not require low-latency stream processing.
Bidirectional Editing Workflow: Rejected since source demo files are treated as immutable evidence.

Consequences:

Positive: Deterministic and reproducible outputs; easier debugging and auditability of parsing behavior.
Negative: Processing cost occurs at ingest time and can increase with larger demo files.

ADR 004: Fail-Soft Parsing and Filtering Policy
Status: Accepted

Context: Demo metadata and extracted entities can contain missing values or irregularities. A fail-fast strategy would stop analyst workflows and reduce utility.

Decision: Apply fail-soft handling in ingestion and filtering. Invalid or partial records are skipped or flagged with clear user-facing feedback instead of crashing the full pipeline.

Alternatives Considered:

Strict Fail-Fast Validation: Rejected because one malformed section could block all analysis.
Silent Error Suppression: Rejected because hidden data loss reduces trust in results.

Consequences:

Positive: Higher robustness for real-world demo variance; analysts can still obtain partial, useful outputs.
Negative: Additional logic required for warning/reporting paths and skipped-record accounting.
