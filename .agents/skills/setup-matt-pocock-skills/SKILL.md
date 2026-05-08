---
name: setup-matt-pocock-skills
description: Sets up an `## Agent skills` block in AGENTS.md and `docs/agents/` so the engineering skills know this repo's issue tracker, triage label vocabulary, and domain doc layout. Run explicitly before first use of `to-issues`, `to-prd`, `triage`, `diagnose`, `tdd`, `improve-codebase-architecture`, or `zoom-out`, or when those skills are missing repository context.
---

# Setup Matt Pocock's Skills For Codex

Scaffold the per-repo configuration that the engineering skills assume:

- **Issue tracker** - where issues live: GitHub, GitLab, local markdown, or a project-specific workflow.
- **Triage labels** - the strings used for the five canonical triage roles.
- **Domain docs** - where `CONTEXT.md` and ADRs live, and the consumer rules for reading them.

This is a prompt-driven skill, not a deterministic script. Explore, present what you found, confirm with the user, then write.

## Codex Adaptation

Codex project instructions live in `AGENTS.md`. Treat `CLAUDE.md` only as an optional legacy source to read while migrating; do not create or update `CLAUDE.md` unless the user explicitly asks for Claude compatibility.

## Process

### 1. Explore

Look at the current repo to understand its starting state. Read whatever exists; don't assume:

- `git remote -v` and `.git/config` - is this a GitHub or GitLab repo? Which one?
- `AGENTS.md` at the repo root - does it exist? Is there already an `## Agent skills` section?
- `CLAUDE.md` at the repo root - if present, read it as legacy context only.
- `CONTEXT.md` and `CONTEXT-MAP.md` at the repo root.
- `docs/adr/` and any `src/*/docs/adr/` directories.
- `docs/agents/` - does this skill's prior output already exist?
- `.scratch/` - sign that a local-markdown issue tracker convention is already in use.

### 2. Present Findings And Ask

Summarise what's present and what's missing. Then walk the user through the three decisions one at a time - present a section, get the user's answer, then move to the next. Don't dump all three at once.

Assume the user does not know what these terms mean. Each section starts with a short explainer: what it is, why these skills need it, and what changes if they pick differently. Then show the choices and the default.

**Section A - Issue tracker.**

Explainer: the issue tracker is where issues live for this repo. Skills like `to-issues`, `triage`, `to-prd`, and `qa` read from and write to it. They need to know whether to call `gh issue create`, use GitLab, write a markdown file under `.scratch/`, or follow some other workflow.

Default posture: these skills were designed for GitHub. If a `git remote` points at GitHub, propose that. If a `git remote` points at GitLab, propose GitLab. Otherwise offer:

- **GitHub** - issues live in GitHub Issues, using the `gh` CLI or the GitHub connector when available.
- **GitLab** - issues live in GitLab Issues, using the `glab` CLI.
- **Local markdown** - issues live as files under `.scratch/<feature>/` in this repo.
- **Other** - ask the user to describe the workflow in one paragraph and record it as freeform prose.

**Section B - Triage label vocabulary.**

Explainer: when `triage` processes an incoming issue, it moves it through a state machine: needs evaluation, waiting on reporter, ready for an AFK agent, ready for a human, or won't fix. It needs the exact labels or equivalent statuses this repo actually uses.

The five canonical roles:

- `needs-triage` - maintainer needs to evaluate.
- `needs-info` - waiting on reporter.
- `ready-for-agent` - fully specified and agent-ready.
- `ready-for-human` - needs human implementation.
- `wontfix` - will not be actioned.

Default: each role's string equals its name. Ask whether the user wants to override any.

**Section C - Domain docs.**

Explainer: some skills (`improve-codebase-architecture`, `diagnose`, `tdd`) read `CONTEXT.md` to learn the project's domain language and `docs/adr/` for past architectural decisions. They need to know whether the repo has one global context or multiple contexts.

Confirm the layout:

- **Single-context** - one `CONTEXT.md` plus `docs/adr/` at the repo root.
- **Multi-context** - `CONTEXT-MAP.md` at the root points to per-context `CONTEXT.md` files.

### 3. Confirm And Edit

Show the user a draft of:

- The `## Agent skills` block to add to `AGENTS.md`.
- The contents of `docs/agents/issue-tracker.md`, `docs/agents/triage-labels.md`, and `docs/agents/domain.md`.

Let them edit before writing.

### 4. Write

Create `AGENTS.md` if it does not exist. If it already has an `## Agent skills` block, update that block in place rather than appending a duplicate. Do not overwrite user edits in surrounding sections.

The block:

```markdown
## Agent skills

### Issue tracker

[one-line summary of where issues are tracked]. See `docs/agents/issue-tracker.md`.

### Triage labels

[one-line summary of the label vocabulary]. See `docs/agents/triage-labels.md`.

### Domain docs

[one-line summary of layout - "single-context" or "multi-context"]. See `docs/agents/domain.md`.
```

Then write the three docs files using the seed templates in this skill folder as a starting point:

- [issue-tracker-github.md](./issue-tracker-github.md) - GitHub issue tracker.
- [issue-tracker-gitlab.md](./issue-tracker-gitlab.md) - GitLab issue tracker.
- [issue-tracker-local.md](./issue-tracker-local.md) - local-markdown issue tracker.
- [triage-labels.md](./triage-labels.md) - label mapping.
- [domain.md](./domain.md) - domain doc consumer rules and layout.

For other issue trackers, write `docs/agents/issue-tracker.md` from scratch using the user's description.

### 5. Done

Tell the user the setup is complete and which engineering skills will now read from these files. Mention they can edit `docs/agents/*.md` directly later; re-running this skill is only necessary if they want to switch issue trackers or restart from scratch.