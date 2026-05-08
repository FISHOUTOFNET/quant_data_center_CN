---
name: obsidian-vault
description: Search, create, and manage notes in an Obsidian vault with wikilinks and index notes. Use when the user wants to find, create, or organize notes in Obsidian, and ask for the vault path if it is not already known.
---

# Obsidian Vault

## Vault Location

Do not assume a fixed vault path. Resolve the vault path in this order:

1. A path explicitly provided by the user.
2. The `OBSIDIAN_VAULT_PATH` environment variable.
3. A repo or user note that documents the vault path.

If none is available, ask the user for the vault path before reading or writing notes.

## Naming Conventions

- **Index notes** aggregate related topics, for example `Skills Index.md` or `RAG Index.md`.
- Use **Title Case** for note names unless the vault already uses another convention.
- Prefer Obsidian links and index notes over inventing folder structures.

## Linking

- Use Obsidian `[[wikilinks]]` syntax: `[[Note Title]]`.
- Notes link to dependencies or related notes near the bottom.
- Index notes are lists of `[[wikilinks]]` with short context where helpful.

## Workflows

### Search For Notes

Use fast filesystem search. Prefer `rg` when available:

```bash
rg --files "$OBSIDIAN_VAULT_PATH" | rg -i "keyword"
rg -n "keyword" "$OBSIDIAN_VAULT_PATH" -g "*.md"
```

### Create A New Note

1. Confirm the vault path.
2. Use Title Case for the filename unless local convention differs.
3. Write content as a unit of learning.
4. Add `[[wikilinks]]` to related notes.
5. If part of a numbered sequence, follow the vault's existing numbering scheme.

### Find Related Notes

Search for backlinks:

```bash
rg -n "\[\[Note Title\]\]" "$OBSIDIAN_VAULT_PATH" -g "*.md"
```

### Find Index Notes

```bash
rg --files "$OBSIDIAN_VAULT_PATH" | rg "Index.*\.md$"
```