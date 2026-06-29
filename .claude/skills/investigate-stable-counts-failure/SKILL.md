---
name: investigate-stable-counts-failure
description: Investigate a stable counts validation failure for a state supervision or incarceration period table by identifying which fields changed and why. Use when a stable counts validation is failing or has recently failed for a state entity (e.g. "why is the NC stable counts failing", "investigate stable counts for US_ID supervision periods", "dig into the stable counts failure for supervision periods").
---

# Skill: Investigate Stable Counts Validation Failure

## Overview

Diagnose a stable counts validation failure for a state entity table
(`state_supervision_period` or `state_incarceration_period`). The goal is to
identify *which fields changed*, *when the change started*, and *why* — tracing
the cause to either a code change (new ingest column, changed mapping) or a data
event (raw data re-export, new column from the state partner).

This skill is investigate-only. Do not propose code fixes or open PRs unless
explicitly asked.

## Inputs

You need two things to start:

- `state_code` — e.g. `US_NC`, `US_ID`. **Never investigate ME or CA data
  without explicit user approval.**
- `target_table_name` — one of `state_supervision_period` or
  `state_incarceration_period`.

If either is missing, **ASK the user** before proceeding.

## Step 1: Run the change-detection script

Run the helper script to get a month-by-month overview and per-field change rates:

```bash
uv run python -m recidiviz.tools.ingest.investigations.stable_counts_change_detection \
    --project-id recidiviz-staging \
    --state-code <STATE_CODE> \
    --entity <state_supervision_period|state_incarceration_period>
```

The script outputs two tables:
- **Month Overview**: total period count per month, distinct people, and
  month-over-month change % (entries exceeding ±25% are flagged with `**`).
- **Field Change Rates**: for each field, the % of periods where it differs from
  the previous period for the same person, filtered to fields that exceed 15% in
  at least one month.

Use `--field-threshold 10` to lower the filter, `--lookback-months 24` to widen
the window, or `--output-csv /tmp/out.csv` to save the full data for further
analysis.

**Note on ~100% rates in early months:** Months that predate the state's
onboarding in our system will show ~100% change rates for all fields — this is
expected (those periods have no prior period to compare against). Focus on the
months where the state has been actively sending daily data.

## Step 2: Identify the anomalous months and driving fields

Look for months where the Month Overview flags a `**` change or where the Field
Change Rates show a sudden spike above recent baseline. Key signals:

- A **sudden spike** in a month that was previously stable (baseline ~10-20%,
  spike to 50-90%) is a strong indicator of a data event.
- A **persistent elevation** starting at a particular month often means a code
  change was deployed around then.
- A field with a **high change rate AND zero change in the raw_text counterpart**
  (e.g., `change_in_termination_reason` is high but
  `change_in_termination_reason_raw_text` is 0) means the enum was re-computed
  from unchanged raw text — look for normalization logic changes rather than raw
  data changes.
- A **mass spike in `distinct_people`** in a single month (e.g., 3× the normal
  number of people getting new period starts in one month) usually means a
  large-scale raw data re-export or new column addition triggered period splits
  for the entire active population.

## Step 3: Find the ingest view(s) for the entity

Locate the ingest view(s) that produce the target entity:

```bash
find recidiviz/ingest/direct/regions/<state_code_lower>/ \
  -name "view_supervision_period*.py" -o -name "view_incarceration_period*.py"
```

Read the view to understand:
- Which raw tables it queries (look for `{raw_table_name}` or
  `{raw_table_name@ALL_WITH_DELETED}` references)
- What fields are selected and which raw columns they map to
- Whether the view uses `@ALL_WITH_DELETED` — if so, **every raw row
  (including non-meaningful field changes) creates a new period split**. This
  makes the view sensitive to any column addition in the raw data.

Also read any shared SQL fragment files referenced from the view (e.g.,
`us_nc_view_query_fragments.py`).

## Step 4: Check raw data YAML configs for column additions

For each raw table used by the ingest view, read its YAML config:

```
recidiviz/ingest/direct/regions/<state_code_lower>/raw_data/us_<xx>_<table_name>.yaml
```

Scan the `update_history` entries for each column. The most common cause of
stable counts failures is a **new column being added to the state's file**
(indicated by `update_type: ADDITION`). When a new column arrives:

- If the ingest view uses `@ALL_WITH_DELETED`, **every active record in the
  file gets a new `update_datetime`**, creating a period split for every person
  currently on supervision.
- The date in `update_datetime` of the ADDITION entry is the date the state
  partner started sending the column — cross-reference this with the anomalous
  month from Step 2.

```bash
grep -A 4 "update_type: ADDITION" recidiviz/ingest/direct/regions/<state_code_lower>/raw_data/us_<xx>_<table_name>.yaml
```

## Step 5: Check recent git history

Look for recent commits touching the state's ingest files that might explain
the change:

```bash
git log --oneline --since="<3 months ago>" -- recidiviz/ingest/direct/regions/<state_code_lower>/
```

Key commits to investigate:
- Any commit that adds new raw data columns to the YAML (`update_type: ADDITION`)
- Any commit that changes the ingest view's SELECT list or raw table query
- Any commit that modifies the ingest mapping YAML
- Any commit titled "Update raw data configs and stable counts validation" —
  this is a pattern used when stable counts are intentionally bumped after a
  known change

For any suspicious commit, run `git show <hash> -- <relevant_file>` to see the
diff.

## Step 6: Confirm with raw data volume query

Once you have a hypothesis about *what* changed and *when*, confirm it by
checking the raw data volume for the affected table around the suspected date:

```sql
SELECT
  DATE(update_datetime) as update_date,
  COUNT(*) as cnt,
  COUNT(DISTINCT <primary_key_col>) as distinct_persons,
  COUNTIF(is_deleted) as deleted_cnt
FROM `<project>.us_<xx>_raw_data.<table_name>`
WHERE update_datetime >= '<start_date>'
  AND update_datetime < '<end_date>'
GROUP BY 1
ORDER BY 1
```

A day with dramatically more records than usual (e.g., 10-50× the daily
baseline) is a **mass re-export event** — the state partner re-sent their
entire active caseload, which triggers period splits for every person.

If the anomaly is on a specific date, check what changed in the records for
that day vs. the previous record for the same people:

```sql
WITH before AS (
  SELECT <key_col>, <suspicious_col_1>, <suspicious_col_2>, update_datetime
  FROM `us_<xx>_raw_data.<table_name>`
  WHERE DATE(update_datetime) = '<day_before>'
    AND NOT is_deleted
),
on_date AS (
  SELECT <key_col>, <suspicious_col_1>, <suspicious_col_2>, update_datetime
  FROM `us_<xx>_raw_data.<table_name>`
  WHERE DATE(update_datetime) = '<anomaly_date>'
    AND NOT is_deleted
)
SELECT
  COUNTIF(b.<col1> != a.<col1> OR (b.<col1> IS NULL) != (a.<col1> IS NULL)) as col1_changed,
  COUNTIF(b.<col2> != a.<col2> OR (b.<col2> IS NULL) != (a.<col2> IS NULL)) as col2_changed,
  COUNT(*) as matched_people
FROM before b
JOIN on_date a USING (<key_col>)
```

## Step 7: Synthesize and present findings

Structure your report as follows:

### TLDR
2-3 sentences for a non-engineer audience: what changed, why it happened, and
whether the current data is correct or there's an ongoing issue.

### Timeline
List the key dates:
- When the stable counts failure started firing
- The commit(s) or raw data event(s) that triggered it
- Whether the issue is one-time or ongoing

### Root Cause
Explain the specific mechanism:
- Which raw column(s) caused the period splits
- Whether it was a new column addition (`update_type: ADDITION` in the YAML)
  or a mass re-export, or both
- Why this causes the specific field in `normalized_state` to change (e.g.,
  "TESTDATE/POSITIVE being added to the offenders file caused every active
  supervision record to get a new `update_datetime`, creating new period splits
  for 54,000 people on 4/11. New periods are open (NULL termination_reason)
  while their preceding periods now have a termination_date and get
  INTERNAL_UNKNOWN.")

### Ongoing Impact
State whether the change rate will return to baseline or remains elevated:
- One-time event: "After the mass re-export settled, the change rate returned
  to ~15%, suggesting normal turnover."
- Ongoing: "Since drug test columns update frequently, ~1,500 people/day now
  get new period splits. The stable counts threshold needs to be raised."

### SQL Evidence
For each key finding, include the exact query run and a representative sample
of results.

## Common Issues

These are root causes discovered from past investigations. Check these first
before running the full investigation — the anomaly date and field pattern may
match one directly.

### 1. New column added to a raw file used by an `@ALL_WITH_DELETED` ingest view

**Signature:** A sudden spike in `distinct_people` on a single date (e.g., 3×
normal), with ~1 new period per person, on a day that is otherwise unremarkable.
The anomalous periods often all share the same `start_date`. The spike is
followed by a persistently elevated daily change rate as the new column continues
to update.

**Mechanism:** When a state partner adds a new column to their file, the raw
data importer creates a new row for every record whose column value changed
(NULL → first value counts as a change). Ingest views that use
`{table@ALL_WITH_DELETED}` create one supervision/incarceration period per raw
row, so every person with a record in that file gets a new period starting on
the date of the column addition.

**Key signal in `termination_reason`:** If the new column is not mapped to
any field in the ingest view (tracked in the raw YAML but unused in the SELECT),
you'll often see a high `change_in_termination_reason` with
`change_in_termination_reason_raw_text = 0`. This happens because the old period
gained a `termination_date` (→ INTERNAL_UNKNOWN) while the new period is open
(→ NULL). The raw text never changes because there is no raw termination reason.

**Where to look:** `update_history` entries with `update_type: ADDITION` in the
raw YAML, dated around the anomalous month.

**Real example (US_NC, April 2026):** NC added `TESTDATE` and `POSITIVE` (drug
test columns) to their `offenders` file on 2026-04-11. The raw importer created
new rows for 54,000+ active supervision cases. The `view_supervision_period`
ingest view uses `{offenders@ALL_WITH_DELETED}`, so every person got a new
period starting 4/11. `termination_reason` change rate jumped from ~15% to ~56%
for April start-date periods because old periods ended on 4/11 (INTERNAL_UNKNOWN)
while new periods remain open (NULL). Drug test data continues to update daily,
causing ~1,500 new period splits per day ongoing.

---

## After the Investigation

If you identified a root cause pattern that is not already listed in the Common
Issues section above, **suggest to the user that they run `/maintain_skill_files`
to add it**. Do not update this file autonomously — proposed additions should be
reviewed before becoming part of the standard playbook.

## Important Notes

- **Never investigate ME or CA data** without explicit user approval
- Always use `recidiviz-staging` for investigation unless the user specifies
  `recidiviz-123` (prod)
- The `@ALL_WITH_DELETED` modifier on raw tables in ingest views is the most
  common reason a non-supervision-related column change causes period splits —
  any tracked column change creates a new `update_datetime`, which becomes a
  new period boundary
- The raw data YAML's `update_history` section is the first place to look for
  explanations; it documents when columns were added to or removed from state
  partner files
- "Stable counts" failures are not always bugs — sometimes they correctly
  detect that a large data change occurred. The job is to determine whether the
  change is *expected and correct* vs. *unexpected or erroneous*
