---
name: investigate-pg-ticket
description:
  Investigate a GitHub bug ticket by fetching PII from the go/github-pii doc and
  suggesting BQ investigation queries.
---

# Skill: Investigate Ticket

## Overview

This skill takes a GitHub issue URL from `pulse-data` (where PG tickets are
now filed) or `recidiviz-dashboards` (the legacy source — tickets filed there
before the cutover to `pulse-data` are still supported until they've been
worked through; TODO(#84434) tracks dropping `recidiviz-dashboards` support),
fetches the ticket details, retrieves associated PII from the shared Google Doc,
and suggests specific BigQuery investigation paths based on the ticket context.

## Instructions

### Step 1: Fetch the ticket from GitHub

Extract the repo and issue number from the URL, then fetch the ticket:

```bash
gh issue view <NUMBER> --repo <ORG>/<REPO> --json title,body,labels,comments
```

### Step 2: Fetch PII from go/github-pii Google Doc

Use the Google Docs API with gcloud credentials to retrieve PII details. The
parsing script is stored at
`.claude/skills/investigate-pg-ticket/parse_github_pii_doc.py`.

The parser script detects API errors (e.g. expired auth tokens) and exits with a
clear error message. If it reports an auth error, ask the user to run
`gcloud auth login` and retry.

```bash
ACCESS_TOKEN=$(gcloud auth print-access-token) && \
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://docs.googleapis.com/v1/documents/1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs" \
  | python3 .claude/skills/investigate-pg-ticket/parse_github_pii_doc.py <ISSUE_NUMBER>
```

Replace `<ISSUE_NUMBER>` with the actual issue number (e.g., `12097`).

### Step 3: Extract key details

@.claude/skills/investigate-pg-ticket/extract-key-details.md

If you're not confident which one is the right match, **ASK the user (use
AskUserQuestion tool)** with the candidates you found as options.

### Step 4: Look up person IDs

@.claude/skills/investigate-pg-ticket/look-up-person-ids.md

### Step 5: Investigate and diagnose

Follow the applicable diagnosis path depending on the product area.

**For Workflows (also referred to as Opportunities — they're the same product area):**

@.claude/skills/investigate-pg-ticket/diagnose-workflows.md

**For Tasks:**

@.claude/skills/investigate-pg-ticket/diagnose-tasks.md

**For Insights / Supervision Homepage (SHP):**

@.claude/skills/investigate-pg-ticket/diagnose-insights.md

**If the issue is not relevant to any of these three product areas:** Stop and
inform the user that there are no specific diagnosis instructions for this
product area, rather than trying to fit the issue into one of the above
buckets. The user can decide how to proceed (e.g., investigate manually).

### Step 6: Present diagnosis results

@.claude/skills/investigate-pg-ticket/present-diagnosis-results.md

- **Post comment** — offer to post the entire Step 6 output (TLDR + details +
  diagnosis) as a comment on the GitHub issue. When posting, strip all PII
  (names, external IDs) from the text. Keep SQL evidence blocks but ensure they
  only use person_ids (never external IDs) in their queries and results. Person
  IDs are not PII and are safe to include in GitHub comments.

## Important Notes

- NEVER put PII (names, external IDs) in GitHub issues or commits. Person IDs
  are not PII and are safe to include.
- Always use `recidiviz-123` for investigation unless the user specifies
  otherwise
- Never access data from Maine (US_ME) or California (US_CA)
- The Google Doc ID for go/github-pii is:
  `1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs`
- If gcloud auth fails, ask the user to run `gcloud auth login` first
