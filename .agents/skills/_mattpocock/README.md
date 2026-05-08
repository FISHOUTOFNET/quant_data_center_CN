# Matt Pocock Skills For Codex

Installed from https://github.com/mattpocock/skills at commit b843cb5ea74b1fe5e58a0fc23cddef9e66076fb8.

This project uses Codex repo-scoped skills, so each skill is installed as a direct child of .agents/skills/ with its original references and scripts preserved.

Codex adaptations made during install:

- Flattened the source category layout (ngineering, productivity, misc, personal, deprecated) into Codex-discoverable skill folders.
- Converted slash command references like /tdd to Codex skill references like $tdd.
- Reworked setup-matt-pocock-skills to write Codex AGENTS.md and docs/agents/, treating CLAUDE.md only as legacy migration context.
- Reworked git-guardrails-claude-code into git-guardrails-codex, using Codex hook locations and a cross-platform Python hook script.
- Removed assumptions that subagents can be spawned automatically; Codex versions now require explicit user authorization before using subagents.
- Removed automatic commit instructions unless the user explicitly asks for a commit.
- Generalized the personal Obsidian vault skill so it asks for a vault path instead of using the original author's path.
- Marked deprecated skills as explicit-only via gents/openai.yaml.

Restart Codex if the new project skills do not appear immediately.