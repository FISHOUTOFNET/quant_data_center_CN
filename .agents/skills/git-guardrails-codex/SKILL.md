---
name: git-guardrails-codex
description: Set up Codex hooks to block dangerous git commands such as push, reset --hard, clean, and branch -D before they execute. Use when the user wants repository or user-level Git safety guardrails for Codex.
---

# Setup Git Guardrails For Codex

Sets up a Codex `PreToolUse` hook that intercepts dangerous Git shell commands before Codex executes them.

## What Gets Blocked

- `git push`, including force-push variants.
- `git reset --hard`.
- `git clean -f` and `git clean -fd`.
- `git branch -D`.
- `git checkout .` and `git restore .`.

When blocked, Codex receives a denial message from the hook.

## Codex Notes

Codex hooks are configured under `.codex/hooks.json` or `.codex/config.toml` for a project, and under `~/.codex/hooks.json` or `~/.codex/config.toml` for the user. Hooks require the feature flag:

```toml
[features]
codex_hooks = true
```

Project-local hooks load only when the project `.codex/` layer is trusted.

## Steps

### 1. Ask Scope

Ask whether to install for this project only or for all projects.

- **Project**: `.codex/hooks/block-dangerous-git.py` and `.codex/hooks.json` or `.codex/config.toml`.
- **Global**: `~/.codex/hooks/block-dangerous-git.py` and `~/.codex/hooks.json` or `~/.codex/config.toml`.

### 2. Copy The Hook Script

The bundled cross-platform script is at [scripts/block-dangerous-git.py](scripts/block-dangerous-git.py). Copy it to the chosen hook directory.

### 3. Enable Hooks

Ensure the selected Codex config has:

```toml
[features]
codex_hooks = true
```

If the config file already exists, merge this setting instead of overwriting unrelated settings.

### 4. Add The Hook

Prefer `hooks.json` when the target layer does not already use inline hook tables:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "^Bash$",
        "hooks": [
          {
            "type": "command",
            "command": "python .codex/hooks/block-dangerous-git.py",
            "timeout": 30,
            "statusMessage": "Checking Git command"
          }
        ]
      }
    ]
  }
}
```

For project-local hooks, prefer an absolute command or a git-root based command when practical so the hook works from subdirectories.

If hooks already exist, merge into `hooks.PreToolUse` rather than overwriting other hooks.

### 5. Ask About Customization

Ask if the user wants to add or remove blocked patterns. Edit the copied script accordingly.

### 6. Verify

Run a quick test:

```bash
echo '{"tool_input":{"command":"git push origin main"}}' | python <path-to-script>
```

It should exit with code 2 and print a blocked-command message to stderr.