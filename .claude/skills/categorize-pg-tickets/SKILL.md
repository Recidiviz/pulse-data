---
name: categorize-pg-tickets
description:
  Apply the Linear "Issue Category" label to a state's PG tickets that the PG
  diagnosis bot covered since a given date. Use when the user asks to
  categorize, label, or tag PG tickets for a state (e.g. "label the US_TX PG
  tickets since July 1").
---

# Skill: Categorize PG Tickets

## Overview

Linear has a single-select `Issue Category` label group that records the root
cause of each PG ticket, so the team can see where support effort goes. This
skill finds every ticket for one state that the PG diagnosis bot covered since a
start date, reads the evidence on each one, and applies exactly one category
where the cause is settled.

The skill takes two inputs. Ask for either one the user didn't give:

- **State code**, e.g. `US_TX`. For Idaho, include both `US_ID` and `US_IX`
  tickets; the bot accepts either label.
- **Start date**, e.g. `2026-07-01`.

## Categories

Read the current names and descriptions before starting, since the group can
change:

```
list_issue_labels(team="One Big Team", includeGroups=true)  # filter to parent "Issue Category"
```

The labels are workspace-wide, so they apply to tickets on any team. As of
2026-09-23 the group holds:

| Label | Use when |
|---|---|
| `Issue: Query logic` | A view, criterion, or calculation of ours gave the wrong result from correctly ingested data. |
| `Issue: Ingest` | Our ingest mapped, parsed, or imported raw data wrong, or dropped it. |
| `Issue: State data` | The problem is on the state's side, in the data they send or in their own systems and processes (late or wrong OMS entries, SSO set up wrong). |
| `Issue: Data freshness` | Nothing was wrong; a recent change hadn't reached the product yet and resolved once data caught up. |
| `Issue: Clarification` | Nothing is broken; the reporter needed an explanation of how the product works. |
| `Issue: Front-end` | The bug is in the app UI. |

Never create a label or edit a description in this skill. If a ticket fits no
category, leave it unlabeled and tell the user; they decide whether the group
needs a new entry.

## Instructions

Keep working files in a per-state subdirectory of the scratchpad. The GitHub API
drops connections on long loops, so wrap every `gh` call in a retry.

### Step 1: Read the bot's label check

The bot only runs on issues whose labels pass the `if:` check in
[`.github/workflows/pg-diagnosis.yml`](../../../.github/workflows/pg-diagnosis.yml)
(mirrored in recidiviz-dashboards' `pg-diagnosis-dispatch.yml`). Read it rather
than assuming its contents. At the time of writing it requires `Team: State Pod`,
a `Region:` label for one of the covered states, and `Project: Workflows`,
`Project: Tasks`, or `Project: Insights`. If the state isn't in the check, stop
and tell the user the bot doesn't cover it.

### Step 2: Find the tickets in scope

A ticket is in scope if either holds, in `Recidiviz/pulse-data` or
`Recidiviz/recidiviz-dashboards`:

1. It has a bot diagnosis comment (body starts with `<!-- pg-diagnosis-agent -->`)
   posted on or after the start date.
2. It was created on or after the start date and its current labels pass the
   label check.

For (1), search issues with the Region label, commented on by
`helperbot-recidiviz`, updated since the start date, then read each one's
comments for the marker. GitHub search caps at 1,000 results, and states with
many automated alert issues (e.g. `US_MI`) exceed it. When that happens, narrow
the search to issues that also carry one of the Project labels, since the bot
never runs without one.

```bash
gh search issues --repo Recidiviz/<REPO> --label "Region: <STATE>" \
  --commenter helperbot-recidiviz --updated ">=<DATE>" --limit 1000 \
  --json number,repository
gh api --paginate "repos/Recidiviz/<REPO>/issues/<N>/comments?per_page=100" \
  --jq '.[] | select(.body | startswith("<!-- pg-diagnosis-agent -->")) | .created_at'
```

For (2), run one search per Project label with all three required labels and
`--created ">=<DATE>"`.

List the tickets that pass the label check but have no diagnosis separately. The
usual causes are a Cloud Build timeout or an issue body too large for the build;
check with `gcloud builds list --project=recidiviz-staging --region=us-west1`
near the issue's creation time if the user wants to know why.

### Step 3: Gather the evidence for each ticket

From GitHub, fetch the body, all comments, and the linked PRs in one GraphQL call
(timeline items `CROSS_REFERENCED_EVENT`, `CONNECTED_EVENT`, `CLOSED_EVENT`). For
each fix PR, list the files it changes; they decide between categories (see
Step 4).

Then find the Linear ticket. Its ID appears in the issue body or comments as
`OBT-<n>`, but many tickets have since moved to a state team (`ID-`, `TX-`,
`MI-`, …). `get_issue("OBT-<n>")` follows the move and returns the current ID,
status, labels, and attachments; `list_comments` needs the current ID and fails
on the old one. To map many tickets at once, call `list_issues` on the state's
team with `fields=["id","title","status","labels"]` and match on title.

Read the Linear comments for every ticket whose resolution isn't visible on
GitHub. Top-level Linear comments often don't sync, and they are where
resolutions such as "resolved by PM-127" or "closing, this is an IT issue" live.

### Step 4: Decide each category

Apply a category only when the cause is settled, meaning either:

- a person on our side confirmed the cause in a comment, or
- a fix PR merged.

Never label from the bot's diagnosis alone; it has been wrong often enough (it
has blamed the display for a naming mix-up, and blamed our ingest for a ticket
the state later fixed). Skip tickets that already carry an `Issue:` label.

When a fix PR merged, the files it changes usually settle Ingest versus Query
logic:

| Files changed | Category |
|---|---|
| `recidiviz/ingest/direct/regions/<state>/…` (ingest views, enum parsers, mappings) | `Issue: Ingest` |
| `recidiviz/task_eligibility/…`, `recidiviz/calculator/query/…`, reference CSVs such as contact standards | `Issue: Query logic` |
| `recidiviz-dashboards` frontend code | `Issue: Front-end` |

Leave these unlabeled and say why:

- Feature requests, policy change requests, data requests to the state, and
  internal infrastructure work. The group only categorizes PG bug reports.
- Tickets still waiting on a policy answer or on the reporter.
- Tickets where the fix PR is still open or was closed without merging.

When a call is close (for example, State data versus Data freshness for a change
the state entered the same day the ticket was filed), pick the one the
resolving person's comment supports and list it as a close call in the report.

### Step 5: Apply the labels

Use `save_issue` with `addLabels=["Issue: <Category>"]`. It appends, so the
ticket's other labels stay. Don't pass `labels`, which replaces the full set.

### Step 6: Report back

Give the user:

1. A table of the tickets labeled: Linear ID, GitHub issue, label, and a one-line
   reason with the fix PR linked.
2. Close calls, with the alternative category.
3. The unlabeled tickets, grouped by likely category where one is evident, then
   the ones no category fits.
4. Gaps in the bot's coverage you noticed along the way: tickets that look like
   PG bug reports but fail the label check (missing `Team: State Pod`, or a
   Project label the check doesn't accept, such as `Project: Classification`),
   and in-scope tickets with no diagnosis.
