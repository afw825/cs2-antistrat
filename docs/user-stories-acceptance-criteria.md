# User Stories and Acceptance Criteria

## US-01 Demo Ingestion

As an analyst, I want to upload a CS2 demo so that I can extract player telemetry for analysis.

Acceptance Criteria:

- User can upload `.dem` file from UI.
- File is parsed using demoparser2.
- Only alive player ticks are processed.
- Data is downsampled to 1 tick/second.

## US-02 Side and Round Filtering

As an analyst, I want side/round/player filters so that I can focus on specific tactical setups.

Acceptance Criteria:

- User can filter side by CT, T, or Both.
- User can filter by rounds and players in analysis view.
- Empty filters display a clear "no data" message.

## US-03 Persistence and Querying

As an analyst, I want parsed telemetry saved in a database so that I can re-query without reparsing.

Acceptance Criteria:

- Parsed ticks are persisted to normalized relational schema.
- Match, round, player, and map entities are linked correctly.
- Stored rows are queryable for visualization.

## US-04 Operational Reliability

As a maintainer, I want health checks and logs so that I can monitor service availability and failures.

Acceptance Criteria:

- Streamlit health endpoint `/_stcore/health` returns 200.
- Application emits structured logs.
- Optional Sentry integration can be enabled by environment variables.

## US-05 Engineering Quality Gates

As a team member, I want pre-commit and CI checks so that only quality code merges to main.

Acceptance Criteria:

- Pre-commit runs formatter, linter, and test suite.
- CI runs formatter check, lint check, and tests on PRs.
- Security scanning and dependency update automation are enabled.
