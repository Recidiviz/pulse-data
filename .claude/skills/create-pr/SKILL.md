---
name: create-pr
description:
  Create a GitHub PR for the current branch with auto-generated description. Use
  when the user asks to create a pull request, open a PR, or put up changes for
  review.
---

# Skill: Create PR

## Overview

This skill creates a GitHub PR for committed changes on the current branch.

## Instructions

### Step 1: Verify Clean State

```bash
git status
```

If there are uncommitted changes, ask the user if they want to commit them first
(use the `commit` skill) or proceed without them.

### Step 2: Get Current Branch

```bash
git branch --show-current
```

If on `main`, tell the user they need to be on a feature branch to create a PR.

### Step 3: Determine Base Branch

The base branch is what the PR will merge into.

**Check if GitChildBranchHelpers is installed:**

```bash
ls .git/child_branch_helper/ 2>/dev/null
```

If installed, get the parent branch:

```bash
cbi  # "child branch info" - shows parent branch
```

If NOT installed, check for potential parent branches:

```bash
git log --oneline HEAD ^main --decorate | head -20
```

Look for other branch names in the output. If you see branches other than the
current one, ask the user:

"I see these branches in your history: <branches>. Should the PR target `main`
or one of these branches?"

Default to `main` if no other branches found or user confirms.

### Step 4: Analyze Changes for PR

Get all changes that will be in this PR:

```bash
git log --oneline <current-branch> ^<base-branch>
git diff <base-branch>..HEAD --stat
git diff <base-branch>..HEAD
```

### Step 5: Determine PR Title

**If single commit:** Use the commit message title if it follows the format
`[Tag] Description` (e.g., `[US_IX] Add eligibility criteria`).

**If multiple commits:** Look for a commit with a good title format. If none
exist, generate a title that summarizes all changes.

Format: `[STATE_CODE] Brief description` or `[Component] Brief description`

### Step 6: Gather PR Context from User

Before writing the description, gather information the user knows but Claude
doesn't.

**ASK the user (use AskUserQuestion tool):**

1. **Motivation**: "What motivated this change? Any related tasks, projects, or
   features?"
2. **Related issues**: "Any GitHub issues to link? (e.g., Closes #12345)"
   - If the conversation started with addressing a specific issue, suggest it
   - If the change resolves any in-code TODOs (e.g., `TODO(#12345)`), suggest
     those issues
   - Still prompt in case there are other related issues
3. **Testing confidence**: "How did you verify this works?"

Skip questions where the answer is already known from the conversation.

### Step 7: Determine PR Type Label

Select ONE required type label based on the changes.

Read `.github/pull_request_template.md` to see the available type labels and
their descriptions.

**If unclear which type applies, ASK the user.**

### Step 8: Write PR Description

Write a **concise** description with these sections:

1. **Why**: Brief motivation and context (from Step 6)
2. **What**: The fundamental changes, listed in logical review order.
   Distinguish core changes from resulting refactors.
3. **Testing**: Why you're confident this works

Keep it as short as possible while being clear.

### Step 9: Show PR Draft

Before creating, show the user:

```
Title: <pr-title>
Base branch: <base-branch>
Label: <type-label>
Related issues: <issues>

Description:
<concise description>

Create this PR?
```

Wait for user confirmation.

### Step 10: Push Branch (if needed)

```bash
git push -u origin <branch-name>
```

### Step 11: Create the PR

1. Read `.github/pull_request_template.md` to get the current template
2. Fill in:
   - **Description of the change**: The description from Step 8 +
     `🤖 Generated with help from [Claude Code](https://claude.com/claude-code). If you as a reviewer think this PR description was not helpful / made it harder to review this PR than a human-written description would have, please share your feedback in #ai-productivity`
   - **Related issues**: From Step 6
3. Keep all other sections (Type of change, Checklists) unchanged

```bash
gh pr create --draft --base "<base-branch>" --label "<type-label>" --title "<title>" --body "$(cat <<'EOF'
<filled-in template content>
EOF
)"
```

### Step 12: Return the PR URL

Always end by showing the user the PR URL so they can review it.

## Important Notes

- NEVER force push to main/master
- ALWAYS show draft before creating PR
- ALWAYS use HEREDOC for multi-line PR body to preserve formatting
- If user says "commit and PR", use the `commit` skill first, then this skill
- NEVER include Personally Identifiable Information (PII) in PR descriptions, even when discussing motivations for the change. Avoid names, person IDs, or other identifying details about individuals.
