# GitHub Project Backlog and Sprint Plan

## Backlog

| ID | Story | Acceptance Criteria | Priority |
|----|-------|---------------------|----------|
| US-01 | Ingest CS2 demo telemetry | Parse `.dem`, filter alive players, persist rows | High |
| US-02 | Visualize player positions | Render radar plot by map/round/player filters | High |
| US-03 | Persist queryable match data | Relational schema with maps, matches, rounds, players, ticks | High |

Story files:

- `docs/stories/demo_ingestion.md`
- `docs/stories/side_round_filtering.md`
- `docs/stories/persistence_querying.md`

## Sprint Plan

### Sprint 1

- US-01, US-03
- Deliver parser + loader + schema baseline

### Sprint 2

- US-02
- Deliver visualization UX and filtering workflow
