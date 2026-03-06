# Cowrie Project Instructions

## System Environment
- RTX 3090 (24GB VRAM). Check `nvidia-smi` before loading models.
- Wayland system. Qt apps need `QT_QPA_PLATFORM=xcb`.
- No sudo access. Provide copy-paste commands for root operations.

## Language Defaults
- Primary languages: Go (primary), TypeScript (secondary), Markdown (docs).
- When generating code, default to Go unless told otherwise.

## Git Workflow
- Always run `git remote -v` and `git rev-parse --show-toplevel` before committing or pushing.
- Confirm the correct repo before any git operation.

## Skills & Commands
- Check `.claude/skills/` first when a skill name is referenced.
- If a skill doesn't exist, say so immediately rather than searching the codebase.

## Testing
- 5 language implementations must pass tests before publishing.
- Cross-language fixtures live in `testdata/fixtures/`.
- Go glyph cross-impl tests skip gracefully when `glyph-js` is not built.

## Session Management
- Break work into focused sessions: one major deliverable per session.
- Commit at natural breakpoints before context gets tight.
- Read MEMORY.md at session start for persistent context.
