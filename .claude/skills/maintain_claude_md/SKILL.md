---
name: maintain-claude-md-documentation
description: Proactively update CLAUDE.md documentation when it becomes outdated or incorrect. Use when you notice documented commands, paths, or patterns don't match the actual codebase, when making code changes that invalidate documentation, or when user corrects stale information.
---

# Skill: Maintain CLAUDE.md Documentation

## Overview

This skill defines when and how to proactively update CLAUDE.md files to keep them accurate and current.

**Core principle**: Keep documentation from being wrong. When you notice existing documentation is outdated or becomes stale due to code changes, correct it. This skill is only about fixing incorrect documentation, not adding new documentation (that may be covered by a different skill later).

## When to Update CLAUDE.md

Watch for these signals that documentation needs updating:

### Strong signals (update immediately):

- **You notice outdated information while reading code** - Commands, file paths, patterns, or conventions documented in CLAUDE.md don't match what you see in the actual codebase
- **You make code changes that invalidate documentation** - You rename files, change commands, modify patterns, or update architecture in ways that make existing documentation incorrect
- **User corrects outdated information** - User points out that documentation is wrong or stale
- **You discover documentation contradicts reality** - Testing patterns, file locations, or conventions are different from what's documented

### Out of scope (don't do these):

- **Adding new documentation** - Don't add missing examples, commands, sections, or topics. This skill is only for correcting existing docs. Adding new documentation may be covered by a different skill in the future.
- **Improving clarity** - Don't rewrite docs to be clearer unless the unclear wording caused you to misunderstand something when doing a task

### When NOT to update:

- **Missing documentation** - If docs don't exist for something, don't add them. Only fix what's already there.
- **One-off or temporary information** - Don't document temporary workarounds or one-time tasks
- **Information likely to change frequently** - Avoid documenting things that are in flux
- **User-specific preferences** - Those belong in `.claude/pulse-data-local-settings.md`, not CLAUDE.md
- **Detailed workflow procedures** - Those should be SKILL files, not CLAUDE.md

## What CLAUDE.md Is For

CLAUDE.md files contain:
- Development commands and how to use them
- Code style and conventions
- Testing patterns and locations
- Architecture and component descriptions
- Common patterns across the codebase
- Configuration and setup information

**Locations**:
- `/Users/ageiduschek/src/pulse-data/CLAUDE.md` (root)
- Various subdirectories may have their own CLAUDE.md files

## How to Update CLAUDE.md

### Step 1: Identify what needs updating

When you notice a signal from the "When to Update" section, immediately consider:

- Is the documentation actually wrong or stale?
- Which CLAUDE.md file should be updated?
- What specific information needs to be corrected?
- Is this a minor correction or a major rewrite? (Only do minor corrections proactively)

### Step 2: Propose the change (for corrections only)

For obvious corrections, just mention what you're fixing:

> "I notice the CLAUDE.md says tests live in `recidiviz/test/` but they're actually in `recidiviz/tests/`. I'm correcting that."

OR:

> "I just renamed this file, so I'm updating the CLAUDE.md reference."

### Step 3: Make the update

- Use the Edit tool to update the documentation
- Only fix what's wrong - don't add anything new
- Preserve the existing structure and tone
- Keep it minimal

### Step 4: Confirm the update

After updating:

- Briefly mention what you corrected
- If it was triggered by code changes, note that you've kept the docs in sync

## Example Scenarios

### Scenario 1: Code doesn't match documentation

**Signal**: While reading code, you notice that test files are in `recidiviz/tests/` but CLAUDE.md says `recidiviz/test/`

**Action**:
1. Recognize this is outdated documentation
2. Mention: "I notice the CLAUDE.md has the wrong test directory path. Should I correct it?"
3. If yes, update the path in CLAUDE.md

### Scenario 2: You make a code change that affects documentation

**Signal**: You rename a directory from `recidiviz/pipelines/ingest` to `recidiviz/pipelines/state_ingest`

**Action**:
1. Complete the code change
2. Check if CLAUDE.md references the old path
3. If yes, say: "I'm updating CLAUDE.md to reflect the renamed directory"
4. Update all references to the old path

### Scenario 3: Missing documentation (out of scope)

**Signal**: You notice CLAUDE.md is missing documentation for a feature or command

**Action**:
Don't add it. This skill is only for fixing wrong documentation, not adding new documentation. That may be covered by a different skill in the future.

## Maintaining Documentation After Code Changes

When you make code changes, always consider:

1. **Did I rename or move files?** → Update file path references
2. **Did I change commands?** → Update command examples
3. **Did I modify patterns or conventions?** → Update pattern descriptions
4. **Did I change architecture?** → Update architecture documentation

Make this a habit: after significant code changes, scan the relevant CLAUDE.md for references that need updating.

## Related Documentation

- [Main CLAUDE.md](../../../CLAUDE.md) - The root documentation file
- [Maintain Skill Files](./../maintain_skill_files/SKILL.md) - Skill for maintaining SKILL files
