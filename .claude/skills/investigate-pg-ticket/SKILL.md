---
name: investigate-pg-ticket
description:
  Investigate a GitHub bug ticket by fetching PII from the ticket's private PII
  doc and suggesting BQ investigation queries.
---

# Skill: Investigate Ticket

## Overview

This skill takes a GitHub issue URL from `pulse-data` (where PG tickets are
now filed) or `recidiviz-dashboards` (the legacy source — tickets filed there
before the cutover to `pulse-data` are still supported until they've been
worked through; TODO(#84434) tracks dropping `recidiviz-dashboards` support),
fetches the ticket details, retrieves associated PII from the private Google Doc
linked in the ticket body, and suggests specific BigQuery investigation paths
based on the ticket context.

## Instructions

### Step 1: Fetch the ticket from GitHub

Extract the repo and issue number from the URL, then fetch the ticket:

```bash
gh issue view <NUMBER> --repo <ORG>/<REPO> --json title,body,labels,comments
```

### Step 2: Fetch PII from the ticket's private PII doc

Every ticket gets its own private Google Doc for PII, linked from a banner at the
top of the body you fetched in Step 1:

```
> 🔒 **Private PII doc for this issue → [Open PII doc](https://docs.google.com/document/d/<DOC_ID>/edit)** …
```

Pull `<DOC_ID>` out of that link and fetch the doc with the Google Docs API. The
parsing script is stored at
`.claude/skills/investigate-pg-ticket/parse_github_pii_doc.py`:

```bash
ACCESS_TOKEN=$(gcloud auth print-access-token) && \
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://docs.googleapis.com/v1/documents/<DOC_ID>" \
  | python3 .claude/skills/investigate-pg-ticket/parse_github_pii_doc.py
```

This prints the doc's full text, with `[IMAGE]` standing in for each pasted
screenshot. Read the client's state-issued ID(s) out of it yourself — the doc has
labelled sections, but don't rely on their exact wording, since the template is
generated outside this repo and gets reworded.

The parser detects API errors (e.g. expired auth tokens) and exits with a clear
error message. If it reports an auth error, ask the user to run
`gcloud auth login` and retry.

**If the doc has no client IDs in it** — because nobody filled it in, or because
the only content is `[IMAGE]` screenshots that text extraction can't read —
**stop and tell the user.** Don't fall back to a state-wide or pipeline-level
investigation: an answer about the state's data in general reads as an answer
about this client, which is worse than reporting that you're blocked. Offer the
doc URL so they can read the IDs off the screenshots themselves.

**Legacy tickets (filed before per-ticket docs).** TODO(OBT-44025): delete this
paragraph and its command once every ticket we might still diagnose has its own
PII doc. If the body has no PII doc
banner, the ticket's PII lives in the shared go/github-pii doc instead, keyed by
either the GitHub issue number or the **Linear identifier** (e.g. `OBT-36184`),
depending on when it was filed. The Linear ID is in the `linear-code` linkback
comment fetched in Step 1 (the `<!-- linear-linkback -->` comment, whose
`linear.app/...` URL ends in the identifier). Pass **both** so the lookup
matches regardless of how the entry is keyed — the Linear ID is optional, so omit
it if there's no `linear-code` comment:

```bash
ACCESS_TOKEN=$(gcloud auth print-access-token) && \
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://docs.googleapis.com/v1/documents/1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs" \
  | python3 .claude/skills/investigate-pg-ticket/parse_github_pii_doc.py <GITHUB_ISSUE_NUMBER> <LINEAR_ISSUE_ID>
```

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
- Each ticket's PII doc ID comes from the banner in its own body — there is no
  single doc to look up. The **legacy** shared go/github-pii doc, used only for
  tickets with no banner, is `1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs`
  (TODO(OBT-44025): remove this doc ID with the legacy path)
- Never continue an investigation without client IDs. If the PII doc is empty or
  image-only, say so and stop
- If gcloud auth fails, ask the user to run `gcloud auth login` first
