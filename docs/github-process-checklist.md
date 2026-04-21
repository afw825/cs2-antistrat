# GitHub Process Checklist

Use this checklist to satisfy class GitHub workflow requirements.

## Repository and Story Setup

- Create a semester project repository on GitHub (if not already created).
- Use the `main` branch as the default branch.
- Ensure `docs` exists.
- Create `docs/stories`.
- Store each user story and acceptance criteria in its own markdown file under `docs/stories`.
- Current story files:
  - `docs/stories/demo_ingestion.md`
  - `docs/stories/side_round_filtering.md`
  - `docs/stories/persistence_querying.md`
- Push `main` to GitHub.
- Create one GitHub issue per story where the issue body is only the story file path.

## Branching and Protection

- Create `main` as protected branch.
- Require pull requests before merging.
- Require at least 1 approving review.
- Require status checks to pass before merge:
  - `CI / test-lint`
  - `CodeQL / Analyze`
- Block force pushes and branch deletion.

## Feature Development Flow

- Create feature branch naming convention: `feature/<short-description>`.
- Open a PR from feature branch to `main`.
- Use PR template and request review.
- Merge only after checks pass and review is approved.

## Security and Dependency Hygiene

- Enable Dependabot alerts and auto-generated update PRs.
- Enable CodeQL scanning.
- Enable secret scanning (if available in your plan).

## CI/CD

- CI runs lint + format check + tests for push/PR.
- Optional CD deploy step can be added after hosting target is chosen.

## Code Reviews

- Keep review comments in PR thread.
- Resolve all review conversations before merge.
