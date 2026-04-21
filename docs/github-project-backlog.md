# GitHub Project Backlog and Sprint Plan

## Backlog

| ID | Story | Acceptance Criteria | Priority |
|----|-------|---------------------|----------|
| US-01 | Ingest CS2 demo telemetry | Parse `.dem`, filter alive players, persist rows | High |
| US-02 | Visualize player positions | Render radar plot by map/round/player filters | High |
| US-03 | Persist queryable match data | Relational schema with maps, matches, rounds, players, ticks | High |
| US-04 | Operational health endpoint | `/health` returns status and timestamp | Medium |
| US-05 | Observability and errors | Structured logs + optional Sentry integration | Medium |
| US-06 | Quality gates in git workflow | pre-commit + CI checks for lint/format/tests | High |
| US-07 | Security and dependency scanning | CodeQL + Dependabot configured | Medium |

## Sprint Plan

### Sprint 1

- US-01, US-03
- Deliver parser + loader + schema baseline

### Sprint 2

- US-02, US-04
- Deliver visualization UX and health endpoint

### Sprint 3

- US-05, US-06, US-07
- Deliver logging, pre-commit, CI, and security automation
