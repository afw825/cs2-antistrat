# US-01 Demo Ingestion

## User Story

As an analyst, I want to upload a CS2 demo so that I can extract player telemetry for analysis.

## Acceptance Criteria

- User can upload a `.dem` file from the UI.
- File is parsed using demoparser2.
- Only alive player ticks are processed.
- Data is downsampled to 1 tick/second.

## GitHub Issue Body

Use this exact content in the corresponding GitHub issue:

```text
docs/stories/demo_ingestion.md
```
