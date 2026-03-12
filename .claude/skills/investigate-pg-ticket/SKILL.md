---
name: investigate-pg-ticket
description:
  Investigate a GitHub bug ticket by fetching PII from the go/github-pii doc and
  suggesting BQ investigation queries.
---

# Skill: Investigate Ticket

## Overview

This skill takes a GitHub issue URL from `recidiviz-dashboards` (or
`pulse-data`), fetches the ticket details, retrieves associated PII from the
shared Google Doc, and suggests specific BigQuery investigation paths based on
the ticket context.

## Instructions

### Step 1: Fetch the ticket from GitHub

Extract the repo and issue number from the URL, then fetch the ticket:

```bash
gh issue view <NUMBER> --repo <ORG>/<REPO> --json title,body,labels,comments
```

### Step 2: Fetch PII from go/github-pii Google Doc

Use the Google Docs API with gcloud credentials to retrieve PII details. The
parsing script is stored at
`.claude/skills/investigate-pg-ticket/parse_github_pii_doc.py`:

```bash
ACCESS_TOKEN=$(gcloud auth print-access-token) && \
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://docs.googleapis.com/v1/documents/1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs" \
  | python3 .claude/skills/investigate-pg-ticket/parse_github_pii_doc.py <ISSUE_NUMBER>
```

Replace `<ISSUE_NUMBER>` with the actual issue number (e.g., `12097`).

### Step 3: Extract key details

#### Step 3a: Determine the state code

Look for the state code in the ticket title — it's typically in brackets like
`[US_ID]` or `[US_IX]`. Also check the ticket body for a "State(s)" field.

If neither the title nor the body contains a recognizable state code, **ASK the
user (use AskUserQuestion tool)** with common state codes as options.

#### Step 3b: Determine the product area

Look for the product area in the ticket title — it's typically in brackets like
`[Product]`, `[Tasks]`, `[Workflows]`. Also check the ticket body for a
"Product" field (e.g., "Tasks - contacts", "Workflows", "Insights").

If neither the title nor the body contains a recognizable product area, **ASK
the user (use AskUserQuestion tool)** with options like "Tasks - contacts",
"Workflows", "Insights", "PSI".

#### Step 3c: Find the right table to query

Based on the ticket context (state code, product area, and problem description),
search the codebase to identify the specific eligibility span or record view
that powers the feature in question.

- For **Tasks**, look in
  `recidiviz/task_eligibility/compliance_task_eligibility_spans/<state_code>/`
  for the matching task builder.
- For **Workflows**, look in
  `recidiviz/task_eligibility/eligibility_spans/<state_code>/` for the matching
  task builder, and in
  `recidiviz/calculator/query/state/views/workflows/firestore/` for the record
  view.
- Use keywords from the ticket (e.g., "face to face", "contact", "LSI-R",
  "discharge") to grep for the relevant file.

If you're not confident which table is the right one, **ASK the user (use
AskUserQuestion tool)** with the candidates you found as options.

### Step 4: Present the ticket summary

Present a clear summary with:

- **Ticket title and link**
- **State code**
- **Officer/user** (from PII doc)
- **Affected clients** with external IDs (from PII doc)
- **Problem description**
- **Screenshots** — note if the PII doc contains screenshots or image references
  related to this ticket (the Google Docs API text extraction won't include
  images, so mention that there may be screenshots in the doc and suggest the
  user check the doc directly if visual context would help)

### Step 5: Suggest investigation path

Based on the product area and problem, suggest specific BigQuery tables and
queries to investigate. Use the state code and external IDs from the PII doc.

**Look up person_id from external_id:** Use the external IDs from the PII doc to
look up person_ids via the `state_person_external_id` table:

```sql
SELECT person_id, external_id, id_type, state_code
FROM `recidiviz-staging.state.state_person_external_id`
WHERE external_id IN ('<EXT_ID_1>', '<EXT_ID_2>')
  AND state_code = '<STATE_CODE>'
```

**Note:** In rare cases, two `person_id` values may be associated with the same
`(state_code, external_id)` pair. If this happens, **ASK the user (use
AskUserQuestion tool)** which `id_type` they expect the external ID to be, and
filter accordingly.

**Note on US_ID/US_IX:** This table stores `US_IX` for Idaho, not `US_ID`. If
the ticket is for US_ID, use `state_code = 'US_IX'` in this query (and all
subsequent queries against BQ datasets).

### Step 6: Offer to run the investigation

After presenting the summary and suggested queries, offer to run the queries
against BigQuery to diagnose the issue. Use `bq query` or the BigQuery MCP
server if available.

### Step 7: Run queries and diagnose the problem

Run the queries identified in steps 5 and 6 using `bq query`.

```sql
-- What does Recidiviz think?
-- Use compliance_task_eligibility_spans_<state_code> for Tasks,
--     task_eligibility_spans_<state_code> for Workflows
SELECT person_id, start_date, end_date, is_eligible, reasons
FROM `recidiviz-staging.<dataset>.<task_name>_materialized`
WHERE person_id IN (<PERSON_IDS>)
  AND CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date, '9999-12-31')
```

After running the queries, diagnose based on what you find:

- **Person not found in eligibility spans?** They may not be in the candidate
  population. Check `task_eligibility_candidates_<state_code>` for that person.
- **is_eligible is wrong?** Look at the `reasons` JSON to identify which
  criteria is failing. Then query the specific criteria span in
  `task_eligibility_criteria_<state_code>.<criteria_name>_materialized` to
  understand why. **Note:** Criteria table names do NOT include a state prefix —
  e.g., the table is `meets_face_to_face_contact_triggers_materialized`, not
  `us_ix_meets_face_to_face_contact_triggers_materialized` (even though the
  dataset name contains the state code).
- **Eligibility looks correct but record view disagrees?** The record view may
  have additional filtering. Read the record view query in the codebase to
  understand the logic.
- **Due date or contact date is wrong?** For Tasks contact issues, check the
  preprocessed contact events view (e.g.,
  `recidiviz-staging.tasks_views.us_ix_contact_events_preprocessed`) to see what
  Recidiviz has ingested vs what the user reports. For Workflows or non-contact
  issues, check the underlying normalized data (e.g.,
  `<state_code>_normalized_state.state_supervision_contact`).
- **Cadence seems wrong or changed unexpectedly?** For contact-related tickets,
  check the cadence and supervision level spans:
  - `recidiviz-staging.tasks_views.<state_code>_contact_cadence_spans_materialized` —
    shows the cadence periods and required contact quantities
  - `recidiviz-staging.tasks_views.<state_code>_case_type_supervision_level_spans_materialized` —
    shows when supervision level changed (which drives cadence changes)

## Important Notes

- NEVER put PII (names, person IDs, external IDs) in GitHub issues or commits
- Always use `recidiviz-staging` for investigation unless the user specifies
  otherwise
- Never access data from Maine (US_ME) or California (US_CA)
- The Google Doc ID for go/github-pii is:
  `1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs`
- If gcloud auth fails, ask the user to run `gcloud auth login` first
- Task names can be found by searching for view builders in
  `recidiviz/task_eligibility/eligibility_spans/` and
  `recidiviz/task_eligibility/compliance_task_eligibility_spans/`
