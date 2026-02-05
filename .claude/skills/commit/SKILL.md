---
name: commit
description: Commit uncommitted changes with clear commit messages. Use when the user
  asks to commit changes, save work, or create a commit.
---

# Skill: Commit Changes

## Overview

This skill commits uncommitted changes with clear, descriptive commit messages.

## Instructions

### Step 1: Analyze Uncommitted Changes

Run these commands to understand what needs to be committed:

```bash
git status
git diff HEAD --stat
git diff HEAD
```

Read any modified files if needed to understand the context of changes.

### Step 2: Determine Target Branch

**If on `main`:**

Prompt the user to create a new branch. Suggest a name based on the changes.

**Check if GitChildBranchHelpers is installed:**

```bash
ls .git/child_branch_helper/ 2>/dev/null
```

If the directory exists, use `cmk` (child make branch):

```bash
cmk <username>/<short-description>
```

Otherwise, use regular git:

```bash
git checkout -b <username>/<short-description>
```

Branch naming convention: `<username>/<kebab-case-description>`

**If not on `main`:**

Ask the user if they want to commit to the current branch or create a new one.
If the user explicitly says "commit to the current branch", skip this prompt.

### Step 3: Stage Changes

Stage all relevant changes:

```bash
git add <files>
```

CRITICAL: Do NOT stage files that contain secrets (.env, credentials.json, etc.)

### Step 4: Generate Commit Message

Create a commit message following this format:

- **Title**: `[STATE_CODE] Brief description` or `[Component] Brief description`
- Keep title under 72 characters
- Use imperative mood ("Add feature" not "Added feature")

Examples:

- `[US_IX] Add CRC-like bed eligibility criteria`
- `[BigQuery] Fix view dependency ordering`
- `[Workflows] Update client record supervision level`

### Step 5: Create the Commit

```bash
git commit -m "$(cat <<'EOF'
<commit message>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

### Step 6: Report What Was Done

After committing, tell the user:

```
✓ Created commit on branch: <branch-name>

Commit: <commit-hash-short>
<commit message title>

Files committed:
  - <file1>
  - <file2>

If you need to make changes:
  - Undo commit (keep changes): git reset --soft HEAD~1
  - Amend commit message: git commit --amend -m "New message"
  - Rename branch: git branch -m <new-name>
```

### Step 7: Push to Remote (Optional)

Ask the user if they want to push. If yes:

**Check if branch has upstream:**

```bash
git rev-parse --abbrev-ref --symbolic-full-name @{u} 2>/dev/null
```

If the branch already has an upstream (command succeeds):

```bash
git push
```

If the branch is new (command fails):

```bash
git push -u origin <branch-name>
```

## Important Notes

- NEVER commit secrets or sensitive files
- NEVER force push to main/master
- ALWAYS use HEREDOC for multi-line commit messages to preserve formatting
- NEVER include Personally Identifiable Information (PII) in commit messages. Avoid names, person IDs, or other identifying details about individuals.
