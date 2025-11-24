---
name: create-pulse-data-github-tasks
description: File GitHub issues and tasks in the pulse-data repository. Use when the user asks to file, create, or track issues, tasks, tickets, or TODOs. Handles issue templates, labels, team assignments, and in-code TODO references.
---

# Skill: Create Recidiviz Data GitHub Tasks

## Overview

This skill defines how to create GitHub issues/tasks in the pulse-data
repository.

## Instructions

**IMPORTANT**: Always get permission from the user before filing GitHub issues.
Discuss task granularity with them - multiple related TODOs should reference the
same issue if they would be addressed as part of the same code change or related
changes in service of a single task.

**Placeholder TODOs**: You can add `TODO(XXXX)` in code as a placeholder before
filing a task. This will cause lint to fail, ensuring the task gets filed before
merging. See [GitHub TODOs documentation](../../../CLAUDE.md#github) for more
details.

### Step-by-step workflow

**Follow these steps IN ORDER. Do not skip ahead to filing the issue before completing all preparatory steps.**

#### 1. Choose and READ the issue template

**CRITICAL**: You MUST read the template file BEFORE filing the issue.

- For most tasks associated with in-code TODOs: `.github/ISSUE_TEMPLATE/general-task.md`
- For bugs: `.github/ISSUE_TEMPLATE/bug_report.md`
- For other specific cases: Check `.github/ISSUE_TEMPLATE/` directory
- If the user told you to use a specific template, use that one

**Use the Read tool to read the template file now.** You need to see the exact structure to match it.

#### 2. Determine the title prefix

Create an informative, concise title with an appropriate prefix:

- If the task is state-specific, prefix with `[US_XX]` where XX is the state code
- Otherwise (or in addition), prefix with a helpful label like:
  - `[Workflows]` for Workflows product-related changes
  - `[BigQuery]` for BQ infra-related changes
  - `[Tools]` for new scripts
  - `[Raw data import]` for raw data import issues
  - `[Ingest views]` for ingest view issues
  - `[Deploy]` for deployment-related tasks
  - etc.

**If the appropriate prefix is not obvious from context, ASK THE USER** what prefix they want rather than guessing.

#### 3. Choose labels

**CRITICAL**: NEVER hallucinate or make up label names. Always verify labels exist using `gh label list` before using them.

- If the task is state-specific, always add the appropriate `Region: US_XX` label
- Every issue MUST have a `Team: *` label
  - **CRITICAL**: ALWAYS ask the user which team label to use. Never assume or add a team label without explicit user confirmation.
  - First, present the most commonly used team labels:
    - `Team: Data Platform`
    - `Team: State Pod`
    - `Team: DSI`
    - `Team: Polaris`
    - `Team: Impact Analytics`
  - If none of those are appropriate, use `gh label list -S Team` to show all available Team labels.
  - Remind the user that they can store personal preferences about what Team label to use by default in `.claude/pulse-data-local-settings.md`.
- For any other labels (e.g., topic-specific labels), use `gh label list -S <keyword>` to verify the label exists before using it
- If a label doesn't exist, omit it from the issue

#### 4. Build the issue body

Format the body to EXACTLY match the template structure you read in step 1.

- Whenever possible, give context about what motivated filing this issue
- If the issue is associated with in-code TODOs, always say "See in-code TODOs"
- Be concise - avoid verbose descriptions
- If you understand the steps needed to complete the task, outline those steps
- **CRITICAL**: ALWAYS add a footnote at the end of the body: `---\n_Filed by Claude_`

#### 5. Show the draft to the user

**Always show the user what you're about to file before filing it.** Display:
- The title you've prepared
- The labels you'll use
- The body you've formatted

For example:
```
I'm ready to file this issue:

Title: [Raw data import] Support configurable EOF character stripping
Labels: Team: Data Platform

Body:
**What needs to be done? Why does it need to be done?**
[body content here]

**Additional context**
[additional context here]
```

#### 6. (Optional) Allow manual editing

**After showing the draft, ask if they want to manually edit it.** Say something like:

> "If you'd like to manually edit the title or body before I file it, just let me know and I'll write it to a temp file for you to edit."

If the user asks to manually edit:
1. Create a temp file at `/tmp/github_issue_draft.md` with the following format:
   ```
   TITLE: [Your title here]

   BODY:
   [Your body here formatted according to template]
   ```
2. Tell the user: "I've written the draft to `/tmp/github_issue_draft.md`. Please edit it and let me know when you're done."
3. Wait for the user to confirm they're done editing
4. Read the temp file back and parse the TITLE and BODY sections
5. Proceed to file the issue with the edited content

#### 7. File the task using the `gh` command-line utility

**ONLY file the issue after completing steps 1-6 above.**

**IMPORTANT**: The `gh` CLI does NOT support using `--template` and `--body`
together. You must format the body text yourself according to the template
structure.

**Example workflow:**

1. User says: "File a task to add EOF stripping for raw data import, use Team: Data Platform"
2. You read `.github/ISSUE_TEMPLATE/general-task.md` to see the structure
3. You determine the prefix should be `[Raw data import]` based on context
4. You note the Team label is `Team: Data Platform` (provided by user)
5. You format the body to match the template's structure exactly
6. You show the user the draft title, labels, and body
7. You ask if they want to manually edit it (if yes, write to temp file and wait for their confirmation)
8. You file with:

```bash
gh issue create \
  --title "[Raw data import] Support configurable EOF character stripping" \
  --label "Team: Data Platform" \
  --body "**What needs to be done? Why does it need to be done?**

Clear description of the task and motivation.

**Additional context**

Any other relevant context. See in-code TODOs.

---
_Filed by Claude_"
```

**Command structure**:

- `--title` or `-t`: Issue title (always include helpful prefix like `[Deploy]`,
  `[US_XX]`, etc.)
- `--body` or `-b`: Issue description formatted according to the template
  structure
- `--label` or `-l`: Add labels (can be used multiple times or comma-separated)
  - **Always include a Team label** (check available teams with:
    `gh issue list --limit 100 --json labels --jq '[.[].labels[].name] | unique | .[] | select(startswith("Team:"))'`)
  - Include `Region: US_XX` for state-specific tasks
- `--assignee` or `-a`: Assign people (use `"@me"` to self-assign)

**Available templates** (for reference only - read them to understand
structure):

- `.github/ISSUE_TEMPLATE/general-task.md` - For most tasks
- `.github/ISSUE_TEMPLATE/bug_report.md` - For bugs
- Others: Check `.github/ISSUE_TEMPLATE/` directory

#### 8. Fill in the issue number in in-code TODOs (if applicable)

After filing the issue, if there are any `TODO(XXXX)` placeholder comments in code, update them to reference the new issue number (e.g. `TODO(#12345)`)

**Note**: Multiple TODOs can reference the same issue number if they're all part
of the same task. For example, if you file issue #12345 for "Update user
authentication flow", you might have:

- `TODO(#12345): Add password hashing` in one file
- `TODO(#12345): Update login endpoint` in another file
- `TODO(#12345): Add authentication tests` in a test file

## Related Documentation

- [GitHub TODOs in code](../../../CLAUDE.md#github)
