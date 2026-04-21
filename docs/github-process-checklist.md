# GitHub Process Checklist

Use this checklist to satisfy class GitHub workflow requirements.

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
