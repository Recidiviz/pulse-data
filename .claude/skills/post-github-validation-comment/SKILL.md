---
name: post-github-validation-comment
description: Post a validation comment to a GitHub PR or issue using the standard format. Use when the user asks to post a comment to a PR or issue, post sandbox or ingest validation results, share validation findings on a PR or ticket, or anything similar that involves posting validation results as a GitHub comment.
---

# Post GitHub Validation Comment

This skill applies to both PRs and issues/tickets. The format is identical; only the target and posting command differ.

## Pick the target

If the user named a PR or issue number, use it. Otherwise auto-detect the PR for the current branch:

```bash
gh pr list --head "$(git branch --show-current)" --json number,title --limit 1
```

Use the result. Only ask the user if the branch has no open PR or they reference a different target. If the target is an issue/ticket (not a PR), the user must provide the number — there is no auto-detect for issues.

## Format

Each comment opens with the current datetime, followed by the setup command that produced the data being compared (sandbox load, ingest run, etc. — rename the heading to match), followed by one analysis block per result. Each analysis block is:

1. **Table** with the headline numbers.
2. **Brief commentary** — a sentence or two on what the table shows and what's caused by this PR vs. upstream drift.
3. **SQL** in a `<details>` block.

**Datetime:** Every comment must open with the current datetime on its own line, formatted as `_YYYY-MM-DD HH:MM TZ_` (italic). Get it from `date '+%F %H:%M %Z'` rather than assuming — do not omit it.

**Keep it brief.** The tables are the deliverable. Commentary is one or two sentences per block, no more. Don't restate the numbers the table already shows, don't add a preamble before the date line, and don't pad with caveats or recaps. Let the data carry the comment.

Template:

````markdown
_<YYYY-MM-DD HH:MM TZ>_

### Sandbox load command

​```
python -m recidiviz.tools.load_views_to_sandbox \
    --sandbox_dataset_prefix <prefix> \
    auto <flags>
​```

### <section title>

| <col> | <col> | <col> |
|---|---|---|
| … | … | … |

<one or two sentences of commentary>

<details><summary>Query</summary>

​```sql
-- the SQL that produced the table above
​```
</details>
````

Repeat the analysis block (table → commentary → SQL toggle) for every result. Do not improvise on the structure.

## One comment per validation

Combine findings from the same setup run into one comment with multiple analysis blocks. Post separate comments only when the setup commands genuinely differ (e.g., a re-load with different sandbox flags or a new ingest after a fix).

## Posting

Compose the body in a temp file so multi-line content survives shell quoting, then post with the command that matches the target type:

```bash
# For a PR:
gh pr comment <PR#> --body-file /tmp/gh_validation_<num>.md

# For an issue/ticket:
gh issue comment <issue#> --body-file /tmp/gh_validation_<num>.md
```

Return the comment URL `gh` prints.

To extend a previous comment instead of stacking a new one, add `--edit-last` — use it to consolidate; use a fresh comment when chronology matters.

## No external IDs or PII

GitHub comments are externally visible. Never include external IDs (e.g. `external_id`, `VSTH_ID`, `Contact_ID`, raw state IDs) or PII (names, DOBs, addresses, free-text notes) in either the table or the SQL `<details>` block. Internal `person_id` values are fine. If a specific row matters, aggregate (counts/deltas) or replace the identifier with a placeholder; share specifics out-of-band if needed.
