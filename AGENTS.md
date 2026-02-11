# AGENTS.md (Repo Rules)

These rules apply to any agent working in `/Users/greg/Dev/git/kameo_remote`.

## GitHub Target (Mandatory)

- ALWAYS use `https://github.com/moofone/...` for branches, PRs, and links.
- NEVER target `tqwewe` (no PRs, no pushes, no links).

Canonical repo: `https://github.com/moofone/kameo_remote`

## Remote Hygiene

Before pushing or creating a PR:

- Run `git remote -v` and confirm:
  - `origin` points to `git@github.com:moofone/kameo_remote.git`
  - Any `upstream` remote (if present) is treated as read-only and must NOT be used for PRs.

When generating a PR link, it must be of the form:

- `https://github.com/moofone/kameo_remote/pull/new/<branch>`

