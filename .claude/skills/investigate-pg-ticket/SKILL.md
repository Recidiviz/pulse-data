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

#### Step 3c: Identify the task or opportunity name

Use keywords from the ticket (e.g., "face to face", "contact", "LSI-R",
"discharge") to grep the codebase and identify the task or opportunity name:

- For **Tasks**, search in
  `recidiviz/task_eligibility/compliance_task_eligibility_spans/<state_code>/`.
- For **Workflows**, search in
  `recidiviz/task_eligibility/eligibility_spans/<state_code>/`.

If you're not confident which one is the right match, **ASK the user (use
AskUserQuestion tool)** with the candidates you found as options.

### Step 4: Look up person IDs

Use the external IDs from the PII doc to look up person_ids via the
`state_person_external_id` table:

```sql
SELECT person_id, external_id, id_type, state_code
FROM `recidiviz-123.state.state_person_external_id`
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

### Step 5: Investigate and diagnose

Follow the applicable diagnosis path depending on the product area.

**For Workflows:**

#### W-i: Query the eligibility span

Query the TES for this person and opportunity using `bq query`:

```sql
SELECT person_id, start_date, end_date, is_eligible, is_almost_eligible,
       reasons, ineligible_criteria
FROM `recidiviz-123.task_eligibility_spans_<state_code>.<task_name>_materialized`
WHERE person_id IN (<PERSON_IDS>)
  AND CURRENT_DATE('US/Eastern') BETWEEN start_date
      AND IFNULL(end_date, '9999-12-31')
```

Check `is_eligible` and `is_almost_eligible` to determine eligibility status. If
the person is not in the table at all, check the candidate population for that
opportunity and understand why they are not even a candidate.

#### W-ii: Is their eligibility status wrong?

Look at the `reasons` and `ineligible_criteria` fields to identify which
criteria is failing. Then query the specific criteria view in
`task_eligibility_criteria_<state_code>.<criteria_name>_materialized` to
understand why. **Note:** Criteria table names do NOT include a state prefix —
e.g., the table is `meets_face_to_face_contact_triggers_materialized`, not
`us_ix_meets_face_to_face_contact_triggers_materialized` (even though the
dataset name contains the state code).

#### W-iii: What about the record view?

The opportunity record view
(`workflows_views.<state_code>_<opportunity_name>_record`) transforms TES data
for the frontend and may filter differently. Read the record view query in
`recidiviz/calculator/query/state/views/workflows/firestore/<state_code>_<opportunity_name>_record.py`
to understand the logic and check if the person appears there or if any relevant
fields (e.g., contact dates) look wrong.

Common causes of discrepancies between TES and the record view:

- **Eligibility subset filtering**: The record view may filter to
  `ELIGIBLE_ONLY` or `ELIGIBLE_AND_ALMOST_ELIGIBLE_ONLY`, excluding people in
  other states.
- **External ID join**: Record views INNER JOIN to `state_person_external_id`
  with a specific `id_type`. Anyone without a matching external ID won't appear.
- **State-specific joins**: Some record views add extra joins (e.g., assessment
  scores, case notes, raw data) that further filter results.

**For Tasks:**

Tasks use `compliance_task_eligibility_spans_<state_code>` (not
`task_eligibility_spans`). Key fields: `is_eligible`, `due_date`,
`last_task_completed_date`, `is_overdue`, `reasons_v2`.

#### T-i: What are the relevant contact requirements?

- **Case type / supervision level**: Query
  `tasks_views.<state_code>_case_type_supervision_level_spans_materialized`.
  Note the current supervision level and case type, as well as any recent
  changes.
- **Contact standards**: Check the static reference CSV at
  `recidiviz/calculator/query/state/views/reference/static_reference_data/<state_code>_contact_standards.csv`
  to verify the expected contact frequency for the person's supervision level
  and case type combination.
- **Contact cadence spans**: Query
  `tasks_views.<state_code>_contact_cadence_spans_materialized` to see the
  cadence periods and required contact quantities for this person. Focus on the
  current date.

#### T-ii: What does the compliance TES say?

Query the compliance task eligibility span for the person and task at the
current time:

```sql
SELECT person_id, start_date, end_date, is_eligible, due_date,
       last_task_completed_date, reasons_v2
FROM `recidiviz-123.compliance_task_eligibility_spans_<state_code>.<task_name>_materialized`
WHERE person_id IN (<PERSON_IDS>)
  AND CURRENT_DATE('US/Eastern') BETWEEN start_date
      AND IFNULL(end_date, '9999-12-31')
```

Pull the following fields and present them:

- **`is_eligible`**: Is this person eligible for the task right now?
- **`due_date`**: When is the next due date?
- **`last_task_completed_date`**: When was the last contact or task completion?
- **`reasons_v2`**: Parse the JSON to understand the criteria evaluation — look
  for contact counts, contact_cadence, and which criteria are met/unmet.
- **`candidate_population_view_builder`**: If the person is not in the TES,
  check which candidate population they are in (e.g.,
  "meets_supervision_span_triggers") and query that view to understand why they
  are not in the TES.

#### T-iii: Is the contact date wrong?

Check what contacts Recidiviz has ingested vs what the user reports:

- **Preprocessed contacts**: Query
  `tasks_views.<state_code>_contact_events_preprocessed_materialized` to see
  what contacts Recidiviz knows about (with remapped contact types).
- **Raw contacts**: Query
  `<state_code>_normalized_state.state_supervision_contact` to see the raw
  ingested data before any remapping or filtering.

If the preprocessed view is missing a contact that exists in raw data, the
contact type remapping or status filtering may be excluding it.

#### T-iv: Is the due date wrong?

Check what the contact cadence is according to Recidiviz vs what the user
reports. With the information from T-i and T-ii you can determine whether our
due_date logic is faulty. If the cadence looks correct but the due date is
wrong, there may be an issue with our due date calculation logic.

#### T-v: Frontend record doesn't match eligibility spans

The Tasks frontend record view
(`workflows_views.<state_code>_supervision_tasks_record_v2_materialized`)
applies additional filtering on top of the compliance task eligibility spans:

- **Due date window**: Only shows tasks with `due_date` within the past
  (overdue) or upcoming 2 months.

Read the record view in
`recidiviz/calculator/query/state/views/workflows/firestore/<state_code>_supervision_tasks_record*.py`
to understand the exact filtering logic.

### Step 6: Present results of your diagnosis

Start with a **TLDR** — a 2-3 sentence plain-language summary of the diagnosis
that anyone (including non-engineers) can understand. This should answer: what
was wrong, why it happened, and whether it's fixed. Avoid jargon, table names,
or implementation details in the TLDR.

Then present the full details in the following format:

- **Ticket title and link**
- **State code**
- **Officer/user**
- **Affected clients**
- **Problem description**
- **Screenshots** — note if the PII doc contains screenshots or image references
  related to this ticket (the Google Docs API text extraction won't include
  images, so mention that there may be screenshots in the doc and suggest the
  user check the doc directly if visual context would help)

- **Diagnosis** — include findings from Step 5 in order and with all relevant
  details. **For EACH key finding, include SQL evidence:**

  Structure each finding as:
  1. **Finding statement** (what you discovered)
  2. **SQL Evidence** - The EXACT query you ran and its results (copied directly from bq output)
  3. **Interpretation** (what it means)

  Format for SQL evidence blocks:
  ```sql
  SELECT supervision_level, start_date, termination_date
  FROM `recidiviz-123.normalized_state.state_supervision_period`
  WHERE person_id = 4857115069950340732

  -- Actual bq output (copy-paste directly):
  -- +-------------------+------------+-----------------+
  -- | supervision_level | start_date | termination_date |
  -- +-------------------+------------+-----------------+
  -- | MAXIMUM           | 2025-09-16 | 2025-12-18      |
  -- | MEDIUM            | 2025-12-19 | NULL            |
  -- +-------------------+------------+-----------------+
  ```

  **CRITICAL:** Always run every query using `bq query` BEFORE including it. Never infer, guess, or hallucinate SQL syntax. If you're unsure about JSON extraction or complex field parsing, run a simpler query first to understand the data structure. Do not include query templates or example queries — only queries you have actually executed and can show real results for.

- **Post comment** — offer to post the entire Step 6 output (TLDR + details +
  diagnosis) as a comment on the GitHub issue. When posting, strip all PII
  (names, external IDs) from the text. Keep SQL evidence blocks but ensure
  they only use person_ids (never external IDs) in their queries and results.
  Person IDs are not PII and are safe to include in GitHub comments.

## Important Notes

- NEVER put PII (names, external IDs) in GitHub issues or commits. Person IDs
  are not PII and are safe to include.
- Always use `recidiviz-123` for investigation unless the user specifies
  otherwise
- Never access data from Maine (US_ME) or California (US_CA)
- The Google Doc ID for go/github-pii is:
  `1hYq--Xw6D5Lu96pSFVGeNu9AuxMNtB5F4ltI2VE9FZs`
- If gcloud auth fails, ask the user to run `gcloud auth login` first
