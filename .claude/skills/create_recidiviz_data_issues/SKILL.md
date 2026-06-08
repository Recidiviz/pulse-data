---
name: create-pulse-data-issues
description: File issues and tasks for the pulse-data repository. Use when the user asks to file, create, or track issues, tasks, tickets, or TODOs. Defaults to filing in Linear (which two-way-syncs to GitHub) and lets you set Linear-specific fields like project and milestone; falls back to filing directly in GitHub when Linear is unavailable. Handles templates, labels, team/project/milestone assignment, parent linking, and in-code TODO references.
---

# Skill: Create Recidiviz Data Issues

## Overview

This skill defines how to file issues/tasks for the pulse-data repository.

Issue tracking has migrated to **Linear** as the primary system, with **two-way
sync** to GitHub. Because sync mirrors an issue from whichever system it was
filed in, you must **file in exactly one system** — never both, or you create
duplicates or a sync conflict.

**Default to Linear.** Filing in Linear lets you set fields GitHub can't —
project, milestone, priority, native parent/sub-issue — and the issue still
shows up in GitHub via sync. Fall back to filing directly in GitHub only when
the Linear MCP server is unavailable (see Step 1).

Key facts about this workspace:

- All pulse-data issues live in the **One Big Team** Linear team (key
  `OBT`), so the team is a constant — there is no per-issue team routing.
- A Linear issue's identifier looks like `OBT-12345`. The codebase TODO lint
  accepts `TODO(OBT-12345)` directly (see
  [`check_todo_format.py`](../../../recidiviz/tools/lint/check_todo_format.py)),
  so there is no need to read back a synced GitHub issue number.
- The `Team: *` and `Region: US_XX` labels already exist on the OBT side, so
  setting them as Linear labels carries them to GitHub via sync.

## Always-on rules

**CRITICAL — NO PII**: Never include Personally Identifiable Information in an
issue (Linear or GitHub). This includes names, dates of birth, addresses, or any
other information that could identify a specific individual in the criminal
justice system. For identifiers: internal `person_id` values are acceptable, but
never include `external_id`, `person_external_id`, `display_id`, or other
external identifiers. If context requires referencing specific data, use
anonymized examples or describe the pattern without real values.

**IMPORTANT — get permission first**: Always get permission from the user before
filing. Discuss task granularity with them — multiple related TODOs should
reference the same issue if they would be addressed as part of the same code
change or related changes in service of a single task.

**Placeholder TODOs**: You can add `TODO(XXXX)` in code as a placeholder before
filing. This fails lint, ensuring the task gets filed before merging. Once
filed, replace it with the real reference — `TODO(OBT-12345)` for a
Linear-filed issue, or `TODO(#12345)` for a GitHub-filed one. See
[GitHub TODOs documentation](../../../CLAUDE.md#github).

## Step-by-step workflow

**Follow these steps IN ORDER. Do not skip ahead to filing before completing the
preparatory steps.**

### 1. Decide where to file (Linear vs. GitHub)

Default to **Linear**. Confirm the Linear MCP server is available (e.g. a
`list_teams`/`list_projects` call succeeds). The MCP may be absent in headless
or cron runs.

- If Linear is available → follow the **Linear flow** (Steps 2–9).
- If the Linear MCP is **not** available → **ask the user** whether they want to
  fix the MCP authentication and file in Linear, or fall back to filing directly
  in GitHub. Only use the **GitHub fallback flow** (Step 10) if they choose it.

### 2. Choose and READ the issue template

**CRITICAL**: You MUST read the template file BEFORE drafting the body, so the
description matches the structure a synced GitHub issue is expected to have.

- For most tasks associated with in-code TODOs: `.github/ISSUE_TEMPLATE/general-task.md`
- For bugs: `.github/ISSUE_TEMPLATE/bug_report.md`
- For other specific cases: check the `.github/ISSUE_TEMPLATE/` directory
- If the user named a specific template, use that one

**Use the Read tool to read the template file now.**

### 3. Determine the title prefix

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

**If the appropriate prefix is not obvious from context, ASK THE USER** rather
than guessing.

### 4. Choose labels

The team is always **One Big Team** (`OBT`) — there is no team to choose. Labels
are how `Team:`/`Region:` information is conveyed, and sync carries them to
GitHub.

**CRITICAL**: NEVER make up label names. Verify a label exists before using it —
`list_issue_labels(team="One Big Team", name="<keyword>")`.

- Every issue MUST have a `Team: *` label.
  - **CRITICAL**: ALWAYS ask the user which team label to use. Never assume it.
  - Present the most commonly used team labels first:
    - `Team: Data Platform`
    - `Team: State Pod`
    - `Team: DSI`
    - `Team: Polaris`
    - `Team: Impact Analytics`
  - If none fit, use `list_issue_labels(team="One Big Team", name="Team:")` to
    show all available `Team:` labels.
  - Remind the user they can store a default `Team:` label in
    `.claude/pulse-data-local-settings.md`.
- If the task is state-specific, add the appropriate `Region: US_XX` label.
- For any other (e.g. topic-specific) label, verify it exists first. If it
  doesn't, omit it.

### 5. Resolve the project

A project is strongly preferred but not required (some tracking issues
legitimately have none). Resolve it as follows, then carry the result into the
draft review (Step 8) so the user always sees and can override it.

**5a. Parent wins.** If the issue is being filed as a sub-issue of a parent/epic,
inherit the **parent's project** (fetch the parent with `get_issue`). Sub-issues
almost always belong to the parent's project. Skip the rest of this step.

**5b. Otherwise, gather candidates with two cheap, targeted calls** — do NOT
paginate the full OBT team (it holds 50+ projects):

1. `list_projects(member="me")` — projects you lead or are an explicit member
   of. (Linear membership is explicit opt-in: filing or being assigned an issue
   does *not* make you a member, so this is a small, high-signal set.) Rank
   these to the top.
2. `list_projects(team="One Big Team", query="<signal>")` — where `<signal>` is
   the strongest cheap cue from the task: the **state code** if it is a
   `[US_XX]` task (e.g. a Missouri task → `query="US_MO"`), otherwise a keyword
   from the title (e.g. `"sentence"`, `"ingest"`, `"tablet"`). Projects are
   state-prefixed and labeled, so this is a reliable, single-call topic match.

**5c. Merge, dedupe, and rank** the candidates: your lead/member projects first,
then started/active projects, then by content match against the title.

**5d. Choose and present.** Auto-pick the top candidate when it is clearly the
right one (e.g. an exact state + topic match, or you lead it and it matches);
otherwise present the top ~3–4 as options. Always also offer:

- **Search all projects** — take a free-text term and run
  `list_projects(team="One Big Team", query="<term>")` (covers projects you are
  not a member of).
- **No project.**

### 6. Resolve the milestone

Only after a project is chosen, and only if relevant.

**6a. Parent wins.** If you inherited the project from a parent (5a) and the
parent has a **milestone**, use the parent's milestone. Skip the rest.

**6b. Otherwise**, fetch the chosen project's milestones
(`list_milestones(project="<project>")`) and pick a default:

- **Relevance first** — if a milestone name clearly matches the task, auto-pick
  it.
- **Soonest as fallback** — if relevance is ambiguous, default to the nearest
  upcoming milestone by target date.

**6c. Always offer**, alongside the default: a different existing milestone,
**create a new milestone**, or **none**. To create one, prompt for a name and
target date and call `save_milestone(project="<project>", name=..., targetDate=...)`,
then use it. If the project has no milestones at all, skip straight to offering
"create new" or "none".

### 7. Build the issue body

Format the description to match the template structure you read in Step 2.

- **Never include PII.**
- Give context about what motivated filing the issue whenever possible.
- If the issue is associated with in-code TODOs, say "See in-code TODOs".
- Be concise; if you understand the steps to complete the task, outline them.
- Write Markdown with literal newlines (the Linear `description` field takes raw
  Markdown — do not escape characters).
- **CRITICAL**: end the body with a footer: `---\n_Filed by Claude_`.

### 8. Show the draft to the user

**Always show the full draft before filing.** Display:

- **Title** (with prefix)
- **Team**: One Big Team (OBT)
- **Status**: Triage (the default), or the status the user named
- **Labels** (including the `Team:` label and any `Region:` label)
- **Project** — and how it was chosen (e.g. "inherited from parent", "you lead
  this + title match", "soonest upcoming")
- **Milestone** — and how it was chosen, or "none"
- **Parent issue** (if any)
- Any other set fields (priority, assignee, estimate)
- **Body**

Auto-picked fields are fine, but nothing gets filed until the user has seen this
draft. Then offer a manual edit: if they want to tweak the title or body, write
it to `/tmp/linear_issue_draft.md` in a `TITLE:` / `BODY:` format, wait for them
to confirm they're done, read it back, and proceed.

### 9. File in Linear

Create the issue with `save_issue`:

- `team`: `"One Big Team"` (always)
- `title`, `description`
- `labels`: the verified label names (e.g. `["Team: Data Platform", "Region: US_MO"]`)
- `project`: the resolved project (omit for "no project")
- `milestone`: the resolved milestone (omit for "none")
- `parentId`: the parent issue identifier (e.g. `OBT-123`) when filing a sub-issue
- `state`: **always `"Triage"`** unless the user specifically asked for a
  different status — in which case use the status they named.
- optionally `priority` (0=None, 1=Urgent, 2=High, 3=Medium, 4=Low),
  `assignee` (`"me"` to self-assign), `estimate`

Capture the returned identifier (`OBT-12345`). Then, if there are any
`TODO(XXXX)` placeholders in code for this task, replace them with
`TODO(OBT-12345)`. Multiple TODOs may reference the same identifier if they are
part of the same task.

### 10. GitHub fallback flow (only when chosen in Step 1)

When Linear is unavailable and the user opts to file directly in GitHub, use the
`gh` CLI. Most of the workflow above still applies — only the Linear-specific
pieces change:

- **Steps 2–4** (template, title prefix, labels) apply unchanged, except verify
  labels with `gh label list -S "<keyword>"` instead of `list_issue_labels`.
  Every issue still needs a `Team: *` label (ask the user) and, if
  state-specific, a `Region: US_XX` label.
- **Steps 5–6** (project, milestone) are **Linear-only** and don't apply.
- **Step 7** (build the body) applies unchanged.
- **Step 8** (draft review + optional manual edit) applies — just omit the
  project/milestone lines from the draft (show title, labels, parent, body).
- **Step 9** (file, then fill in TODOs) applies, with the GitHub
  implementation below: file with `gh issue create`, then replace any
  `TODO(XXXX)` placeholders with `TODO(#<issue_number>)` (note the `#` form,
  not `OBT-`).

**Note**: `gh` does NOT support `--template` and `--body` together — format the
body yourself to match the template.

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

To link a parent issue, add the sub-issue relationship via the GraphQL
`addSubIssue` mutation (works across repos):

```bash
# Node IDs for parent and child
gh issue view <parent_number> --repo <owner/repo> --json id --jq '.id'
gh issue view <child_number> --repo <owner/repo> --json id --jq '.id'

gh api graphql -f query='
mutation {
  addSubIssue(input: { issueId: "<PARENT_NODE_ID>", subIssueId: "<CHILD_NODE_ID>" }) {
    issue { number title }
    subIssue { number title }
  }
}'
```

(Add `replaceParent: true` to replace an existing parent.) Then fill in any
`TODO(XXXX)` placeholders with `TODO(#<issue_number>)`.

## Related Documentation

- [GitHub TODOs in code](../../../CLAUDE.md#github)
- [TODO format lint](../../../recidiviz/tools/lint/check_todo_format.py)
