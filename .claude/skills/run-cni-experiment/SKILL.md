---
name: run-cni-experiment
description: >
  Run a controlled CNI prompt/config experiment: identify documents exhibiting a
  target artifact or error mode, extract against main and a fix branch, diff the
  outputs, run golden eval on both, and produce a structured report. Use when
  you have a hypothesis about a prompt or config change and want to verify it
  worked (sandbox) and didn't regress anything (golden eval).
---

# Skill: Run CNI Experiment

## Overview

This skill runs a controlled experiment to validate a CNI prompt or config
change. It has three layers:

1. **Target set** — documents sampled to exhibit a specific artifact or error
   mode
2. **Sandbox comparison** — extract the same target documents on `main` and on a
   fix branch; diff the outputs field by field against an expected-change spec
3. **Golden eval backstop** — run the labeled eval on both branches; flag any
   accuracy regression

The sandbox comparison answers _did the fix produce the expected change?_ The
golden eval answers _did anything else break?_

**Caveats:**

- Never query US_ME or US_CA data.
- Document text contains PII. Keep all output files local; do not paste note
  text into Slack, GitHub, or Notion.

---

## Phase 1: Gather inputs

Ask the user (in one message) for the following. State defaults; user can
override any.

1. **State code** — e.g., `US_TN`. Never `US_ME` or `US_CA`.
2. **Extractor collection** — e.g., `case_note_employment_info`. If unclear,
   list directories in `recidiviz/documents/config/extractor_collections/`
   filtered by state.
3. **Fix branch name** — the branch implementing the change.
4. **Experiment name** — short slug used in sandbox prefixes and filenames
   (e.g., `tn_employ_null_training`).
5. **Expected change** — describe the before-state and after-state as
   field-specifically as possible. Examples:
   - _"Notes mentioning 'on-the-job training' programs currently return a
     non-null `employer_name`; after the fix they should return NULL."_
   - _"Notes with only past-tense employment language currently populate
     `employment_start_date`; after the fix that field should be NULL."_
   - _"Notes with no housing mention currently return `is_housed = TRUE`; after
     the fix they should return NULL."_ This becomes the success criterion for
     Phase 5.
6. **Target set** — use an existing `sample-cni-documents` Google Sheet (paste
   the URL), or generate one now (default: generate).
7. **Target sample size** — how many documents. Default: **50**.
8. **Baseline source** — use existing staging/prod extraction results as the
   baseline (default: **yes, use existing**), or run a fresh sandbox extraction
   on `main`. Using existing results is faster and avoids the new-document noise
   problem. Run a fresh baseline only if the current staging data is known to be
   stale or on a different code version than `main`.
9. **Document limit for extraction runs** — default: **omit** for small target
   sets (a few dozen people); the extraction completes quickly and omitting it
   ensures the public parsed view is deployed, which Phase 5 queries. Set to
   e.g. `200` only when the entity set is large enough that you need to cap LLM
   cost — but be aware that setting `--document-limit` prevents the public
   parsed view from being deployed (only `__raw`, `__validated`,
   `__unvalidated`, and `__pre_resolution` datasets are created). If you do set
   it, see Phase 5a for the fallback query path. Also disables the
   entity-resolution pass. Applies to all sandbox extraction runs (Phase 3
   fresh baseline if used, and Phase 4 branch extraction always).
10. **Project** — default `recidiviz-staging`.

---

## Phase 2: Assemble the candidate set

If the user provided an existing set of `document_contents_id`s (e.g. from a
prior `sample-cni-documents` sheet), skip to reading it. Otherwise, invoke
[`sample-cni-documents`](../sample-cni-documents/SKILL.md) with all inputs
already known — pass them through without re-prompting the user:

- **State code** → from Phase 1 input #1
- **Extractor collection** → from Phase 1 input #2
- **Sample size** → 2–3× the target count from Phase 1 input #7
- **What you're looking for / filter criteria** → the expected-change
  description from Phase 1 input #5 (semantic criterion → sample-then-read mode)
- **Project** → from Phase 1 input #10

**Important:** sample-then-read selects documents whose *text* matches the
target pattern. It does not verify that the baseline extractor actually produces
the before-state for those documents — that confirmation happens in Phase 3.5.
Treat the Phase 2 output as a *candidate set*, not the final target set.

From the candidate Google Sheet, extract:

- **`person_external_id` values** (deduplicated) → `--root-entity-ids` for the
  extraction runs. The baseline run fetches all documents for these people;
  Phase 3.5 then filters down to the confirmed before-state docs.
- **`document_contents_id` values** → the candidate list Phase 3.5 filters.

Get the `--external-id-type` by querying the metadata table. **Always do
this fresh — do not assume the type from prior runs or from the state code
alone, as the registered types vary by state and can differ from what you'd
expect (e.g., `US_CO` uses `US_CO_OFFENDERID`, not `US_CO_DOC`).**

```sql
SELECT DISTINCT person_external_id_type
FROM `{project}.{state_lower}_document_store_metadata.{collection_name_lower}`
LIMIT 5
```

If the extraction run rejects the id type with a "not registered" error, check
the valid types from the error message and re-run with the correct value. Do
not guess — the error lists all registered types for that state.

---

## Phase 3: Establish the baseline

**Default path — use existing staging/prod results:**

Ask the user to confirm that the current staging extraction is up to date (i.e.,
it reflects `main` and hasn't diverged). Then identify the staging output view:

```bash
bq ls --project_id={project} {state_lower}_document_extraction_results
```

The staging view for the collection (e.g., `case_note_housing_info`) is the
baseline. Record it as `{baseline_dataset}.{baseline_view}`. Skip to Phase 3.5.

This is preferable to a fresh extraction because:
- No LLM cost or wait time.
- The staging results and branch results are never interleaved in time, so
  newly-ingested documents cannot appear as false diff noise.

**Fallback — run a fresh sandbox extraction on `main`:**

Use this only if staging results are known to be stale or on a different code
version. Run after confirming with the user (input #8 above).

```bash
git stash   # or switch to main cleanly
git checkout main

uv run python -m recidiviz.tools.documents.run_sandbox_extraction \
  --project-id {project} \
  --sandbox-prefix {experiment_name}_main \
  --state-code {STATE_CODE} \
  --collection {COLLECTION_NAME_UPPER} \
  --external-id-type {id_type} \
  --root-entity-ids {id_1} {id_2} ... \
  --document-limit {document_limit} \
  --labels reason=experiment \
  --labels experiment={experiment_name} \
  --labels branch=main
```

The run writes results to
`{experiment_name}_main_{state_lower}_document_extraction_results`. Wait for it
to complete before running Phase 3.5.

---

## Phase 3.5: Confirm the before-state against baseline output

Text-based sampling (Phase 2) finds documents that *look like* they should
exhibit the bug. This step confirms which of those candidate documents actually
do by checking the baseline extraction results. **Do not run the branch
extraction until you have enough confirmed before-state documents.**

### 3.5a. Identify the baseline view

- **Using staging/prod (default):** the baseline view is
  `{project}.{state_lower}_document_extraction_results.{collection_name_lower}`
  (confirmed in Phase 3).
- **Using a fresh sandbox extraction:** list the sandbox dataset to confirm the
  view name:
  ```bash
  bq ls --project_id={project} {experiment_name}_main_{state_lower}_document_extraction_results
  ```
  The parsed view is typically named after the collection in lowercase.

### 3.5b. Query baseline output for the candidate doc_ids

Translate the expected before-state into a WHERE predicate — e.g., for "should
return `primary_status = 'unhoused'` but currently does":

```sql
SELECT document_contents_id, {field_1}, {field_2}, ...
FROM `{project}.{experiment_name}_main_{state_lower}_document_extraction_results.{view_name}`
WHERE document_contents_id IN (
  '{candidate_doc_id_1}',
  '{candidate_doc_id_2}',
  ...
)
  AND {before_state_predicate}  -- e.g., primary_status = 'unhoused'
```

Note: if the output view filters to `is_relevant = TRUE`, documents where the
baseline returns `is_relevant = FALSE` will simply be absent from the view —
they won't appear with `is_relevant = FALSE`. To capture those:

```sql
-- Use LEFT JOIN from a target_ids CTE so disappearing docs register as absent
WITH target_ids AS (
  SELECT document_contents_id FROM UNNEST([
    '{candidate_doc_id_1}',
    ...
  ]) AS document_contents_id
)
SELECT
  t.document_contents_id,
  v.{field_1},
  v.is_relevant IS NULL AS absent_from_view
FROM target_ids t
LEFT JOIN `{project}.{experiment_name}_main_{state_lower}_document_extraction_results.{view_name}` v
  USING (document_contents_id)
WHERE v.{before_state_field} = '{before_state_value}'
   OR v.document_contents_id IS NULL  -- include docs that disappeared from view
```

### 3.5c. Evaluate the confirmation count

Count how many candidate docs confirmed the before-state. If the count is ≥ the
target sample size, trim to the target count; those are the **confirmed target
docs**. If fewer than the target count confirmed:

1. Note which candidates did *not* exhibit the before-state (they were already
   handled correctly — exclude them from all further phases).
2. Go back to Phase 2: invoke `sample-cni-documents` again with a fresh
   `ORDER BY RAND()` pull to get more candidates for the same or additional
   people. Add those people to `--root-entity-ids` and re-run the baseline
   (or query the already-materialized baseline for any new people added — re-run
   only if they weren't covered).
3. Repeat until you have at least the target count of confirmed before-state docs.

Record the **confirmed target docs** (`document_contents_id` list) — Phase 4
and Phase 5 use this list, not the original candidate list.

---

## Phase 4: Run experimental extraction (fix branch)

```bash
git checkout {fix_branch}

uv run python -m recidiviz.tools.documents.run_sandbox_extraction \
  --project-id {project} \
  --sandbox-prefix {experiment_name}_branch \
  --state-code {STATE_CODE} \
  --collection {COLLECTION_NAME_UPPER} \
  --external-id-type {id_type} \
  --root-entity-ids {id_1} {id_2} ... \
  --document-limit {document_limit} \
  --labels reason=experiment \
  --labels experiment={experiment_name} \
  --labels branch={fix_branch}
```

Writes results to
`{experiment_name}_branch_{state_lower}_document_extraction_results`.

---

## Phase 5: Compare sandbox outputs

### 5a. Look up the output view name and extraction fields

The branch view is in the sandbox dataset:

```bash
bq ls --project_id={project} {experiment_name}_branch_{state_lower}_document_extraction_results
```

The parsed view is typically named after the collection in lowercase (e.g.,
`case_note_housing_info`). Confirm.

**If the public view dataset doesn't exist** (because `--document-limit` was
set), use the pre-resolution materialized table instead:

```
{experiment_name}_branch_{state_lower}_document_extraction_results__pre_resolution.{collection_name}_materialized
```

Note: this table includes unvalidated results, so it is a slightly different
population than the public view — results that failed confidence or consistency
checks will appear here but not in the public view. Account for this when
interpreting diffs.

Find the extraction fields from the collection schema:

```
recidiviz/documents/config/extractor_collections/{collection_lower}/collection.yaml
```

These are the fields to compare (`{field_1}`, `{field_2}`, ...).

### 5b. Build and run the comparison query

The query starts from the confirmed target doc_ids (Phase 3.5) and LEFT JOINs
both the baseline and experimental views. A LEFT JOIN is required because the
output view only shows rows where `is_relevant = TRUE` — a document where the
branch sets `is_relevant = FALSE` disappears entirely from the view rather than
appearing with that value. Starting from a target_ids CTE ensures those
disappearances register as NULL fields instead of silently dropping from the
diff.

Construct this query, substituting field names, view addresses, and the confirmed
target `document_contents_id` list from Phase 3.5. **Include this query in the
report (Phase 8)** so the reader can re-run it without Claude.

```sql
WITH target_ids AS (
  -- confirmed before-state docs from Phase 3.5
  SELECT document_contents_id FROM UNNEST([
    '{doc_id_1}',
    '{doc_id_2}',
    ...
  ]) AS document_contents_id
),
baseline AS (
  SELECT
    document_contents_id,
    {field_1},
    {field_2},
    -- repeat for all extraction fields
  FROM `{baseline_view_address}`  -- staging/prod view or sandbox main view
),
experimental AS (
  SELECT
    document_contents_id,
    {field_1},
    {field_2},
    -- repeat for all extraction fields
  FROM `{project}.{experiment_name}_branch_{state_lower}_document_extraction_results.{view_name}`
),
diffs AS (
  SELECT
    t.document_contents_id,
    b.{field_1}                                       AS {field_1}_baseline,
    e.{field_1}                                       AS {field_1}_experimental,
    b.{field_1} IS DISTINCT FROM e.{field_1}          AS {field_1}_changed,
    b.{field_2}                                       AS {field_2}_baseline,
    e.{field_2}                                       AS {field_2}_experimental,
    b.{field_2} IS DISTINCT FROM e.{field_2}          AS {field_2}_changed,
    -- repeat for all fields
  FROM target_ids t
  LEFT JOIN baseline b USING (document_contents_id)
  LEFT JOIN experimental e USING (document_contents_id)
)
SELECT *
FROM diffs
WHERE {field_1}_changed
   OR {field_2}_changed
   -- repeat for all fields
ORDER BY document_contents_id
```

Save the query to `/tmp/cni_experiment_{experiment_name}_diff.sql` and run it:

```bash
bq query \
  --project_id={project} \
  --use_legacy_sql=false \
  --format=json \
  --max_rows=500 \
  < /tmp/cni_experiment_{experiment_name}_diff.sql \
  > /tmp/cni_experiment_{experiment_name}_diff.json
```

**Parsing note:** `bq query --format=json` writes a `\r`-prefixed progress line
before the JSON array. Strip it before parsing:

```python
import json, re
raw = open("/tmp/cni_experiment_{experiment_name}_diff.json").read()
diff_rows = json.loads(re.sub(r"^\r[^\[]*", "", raw, count=1))
```

Also run a per-field summary. Save it to
`/tmp/cni_experiment_{experiment_name}_summary.sql` and run it. Construct this
by taking the same `WITH target_ids ... diffs AS (...)` CTE from the query
above (omitting the outer `WHERE` clause so all rows are included), then wrap
a `SELECT COUNTIF(...) ...` around it:

```sql
WITH target_ids AS (
  SELECT document_contents_id FROM UNNEST([
    '{doc_id_1}',
    '{doc_id_2}',
    ...
  ]) AS document_contents_id
),
baseline AS (
  SELECT document_contents_id, {field_1}, {field_2} ...
  FROM `{baseline_view_address}`
),
experimental AS (
  SELECT document_contents_id, {field_1}, {field_2} ...
  FROM `{project}.{experiment_name}_branch_{state_lower}_document_extraction_results.{view_name}`
),
diffs AS (
  SELECT
    t.document_contents_id,
    b.{field_1} IS DISTINCT FROM e.{field_1} AS {field_1}_changed,
    b.{field_2} IS DISTINCT FROM e.{field_2} AS {field_2}_changed,
    -- repeat for all fields
  FROM target_ids t
  LEFT JOIN baseline b USING (document_contents_id)
  LEFT JOIN experimental e USING (document_contents_id)
)
SELECT
  COUNTIF({field_1}_changed) AS {field_1}_changed_count,
  COUNTIF({field_2}_changed) AS {field_2}_changed_count,
  -- repeat for all fields
  COUNT(*) AS total_docs_compared
FROM diffs
```

### 5c. Read and annotate the diffs

Read the JSON row by row. For each document where at least one field changed,
classify the change against the expected-change spec from Phase 1:

- **Fixed** — baseline matched the before-state; experimental matches the
  after-state. This is the intended outcome.
- **Regression** — experimental moved in the wrong direction from baseline.
- **Unrelated change** — something changed, but it's not what the spec
  described. Flag for human review.
- **No change** — none of the fields changed on this document. Expected for docs
  that don't exhibit the target artifact.

Tally:
`{fixed_count} fixed / {regression_count} regressions / {unrelated_count} unrelated changes / {unchanged_count} unchanged`.

**Note on unchanged rows:** The diff query's `WHERE field_changed OR ...` clause
filters to rows where at least one field changed, so documents where nothing
changed are **absent from the diff JSON entirely**. After reading the diff JSON,
compute the unchanged set:

```python
diff_ids = {row["document_contents_id"] for row in diff_rows}
unchanged_ids = [doc_id for doc_id in target_doc_ids if doc_id not in diff_ids]
```

Carry `unchanged_ids` forward to Phase 5d — you will need a separate BQ query
to populate their field values in the spot-check sheet.

---

### 5d. (Optional) Write diff results to a Google Sheet

Offer to write the diff results to a Google Sheet for human spot-checking and to
link from the PR comment. Ask the user if they want this before proceeding; it
requires Google Sheets ADC access (same
`gcloud auth login --enable-gdrive-access` as Phase 6).

**What to write:**

**Always create a new Google Sheet** — never update an existing one. Title it
`CNI Experiment: {experiment_name} ({YYYY-MM-DD})`. Write one row per target
document, with these columns (in order):

| Column                   | Source              | Notes                                                                                                 |
| ------------------------ | ------------------- | ----------------------------------------------------------------------------------------------------- |
| `document_contents_id`   | diff output         | SHA256 hash                                                                                           |
| `person_external_id`     | Phase 2 CSV         | external person id from the document store metadata table                                             |
| `document_text`          | BQ (see below)      | **full** document text — not the Phase 2 snippet                                                      |
| `classification`         | 5c annotation       | Fixed / Regression / Unrelated / No change                                                            |
| `claude_annotation`      | 5c analysis         | 1–2 sentence explanation of _why_ the change happened or why it didn't, grounded in the document text |
| `{field_1}_baseline`     | diff output         | repeat for all extraction fields                                                                      |
| `{field_1}_experimental` | diff output         | repeat for all extraction fields                                                                      |
| ...                      |                     |                                                                                                       |

**Fetching full document text:** The Phase 2 CSV contains only a short snippet.
Before building the sheet, query the full text for all confirmed target doc IDs:

```sql
SELECT
  document_contents_id,
  document_text
FROM `{project}.{state_lower}_document_contents.{document_collection_name_lower}_document_contents`
WHERE document_contents_id IN (
  '{doc_id_1}',
  '{doc_id_2}',
  ...
)
```

Join this to the diff output and Phase 2 CSV by `document_contents_id` when
constructing sheet rows.

**Baseline values for unchanged rows:** The diff JSON only contains rows where
at least one field changed (those absent from the diff are the `unchanged_ids`
computed in Phase 5c). For the sheet to be useful, unchanged rows still need
their field values filled in — otherwise those rows appear entirely blank, which
is confusing to reviewers. Query the baseline view for those IDs:

```sql
SELECT
  document_contents_id,
  {field_1},
  {field_2},
  -- repeat for all extraction fields
FROM `{baseline_view_address}`
WHERE document_contents_id IN (
  '{unchanged_doc_id_1}',
  '{unchanged_doc_id_2}',
  ...
)
```

When building a sheet row for an unchanged doc, use these values for **both**
the `{field}_baseline` and `{field}_experimental` columns (they are identical —
nothing changed).

Write the `claude_annotation` column while reading diffs in 5c: for each
document, write a 1–2 sentence note citing the specific phrase or absence of
phrase that drove the change (or non-change). Example: _"The note says 'living
in his car' — baseline incorrectly returned housed=NULL; experimental correctly
returns unhoused."_

**Privacy:** the sheet contains PII (document text, person IDs). **Create it as
a private sheet, visible only to the authenticated Google account.** Do not add
additional viewers or change sharing settings unless the user explicitly asks.

```python
# When creating via the Google Sheets MCP, set no additional permissions —
# the sheet inherits the user's Google Drive defaults (private).
```

Save the sheet URL. The URL itself contains no PII; it is safe to include in the
PR comment even though `pulse-data` is public — anyone without access will
receive a 403. Use the URL in:

- The local report (Phase 8): include the link under a "Spot-check sheet"
  heading.
- The sanitized PR comment (Phase 9): include it in the "Sandbox results"
  section so authorized reviewers can inspect individual diffs.

**Example PR comment entry:**

```markdown
Spot-check sheet (requires data access): [{sheet title}]({sheet_url})
```

---

## Phase 6: Run golden eval on main

The golden eval requires Google Sheets access in ADC. Remind the user to run
this if not already authenticated:

```bash
gcloud auth login --enable-gdrive-access --update-adc
```

Then run on main:

```bash
git checkout main

uv run python -m recidiviz.tools.documents.run_sandbox_golden_eval \
  --project-id {project} \
  --sandbox-prefix {experiment_name}_eval_main \
  --collection {COLLECTION_NAME_UPPER} \
  --state-code {STATE_CODE}
```

Capture the full terminal output — it contains the per-field, per-test-type
accuracy summary. Save it to
`/tmp/cni_experiment_{experiment_name}_eval_main.txt`.

---

## Phase 7: Run golden eval on fix branch

```bash
git checkout {fix_branch}

uv run python -m recidiviz.tools.documents.run_sandbox_golden_eval \
  --project-id {project} \
  --sandbox-prefix {experiment_name}_eval_branch \
  --collection {COLLECTION_NAME_UPPER} \
  --state-code {STATE_CODE}
```

Save output to `/tmp/cni_experiment_{experiment_name}_eval_branch.txt`.

### After both evals: query per-field × test-type accuracy from BQ

The terminal output shows per-field totals and per-test-type totals but not
the cross. The scored rows are written to BQ — run this query on **both** eval
datasets to get the breakdown needed for the report:

```sql
SELECT
  field_name,
  test_type,
  COUNTIF(is_correct)                              AS correct,
  COUNT(*)                                         AS total,
  ROUND(COUNTIF(is_correct) / COUNT(*) * 100, 1)  AS accuracy_pct
FROM `{project}.{experiment_name}_eval_{main_or_branch}_document_extraction_golden_eval_results.{collection_name_lower}`
WHERE run_datetime_utc = (
  SELECT MAX(run_datetime_utc)
  FROM `{project}.{experiment_name}_eval_{main_or_branch}_document_extraction_golden_eval_results.{collection_name_lower}`
)
GROUP BY field_name, test_type
ORDER BY field_name, test_type
```

Save both result sets; use them to build the split tables in Phase 8.

---

## Phase 8: Produce the experiment report

Write `/tmp/cni_experiment_{experiment_name}_{YYYYMMDD}.md`. This is the full
local report and may include document text in representative diffs — it stays on
your machine only.

### Report structure

````markdown
# CNI Experiment: {experiment_name}

**Date**: {YYYY-MM-DD} **State / Collection**: {STATE_CODE} /
{COLLECTION_NAME_UPPER} **Branches**: `main` vs `{fix_branch}`

## Hypothesis

{expected_change_spec from Phase 1 verbatim}

## Sandbox verdict

{fixed_count}/{total_target_docs} target documents showed the expected change.
{regression_count} regressions. {unrelated_count} unrelated changes.

### Per-field change rates (all {total_target_docs} target docs)

| Field     | Changed | %   |
| --------- | ------- | --- |
| {field_1} | {n}     | {%} |
| ...       |         |     |

### Spot-check sheet

{Link to Google Sheet from Phase 5d, if created. Omit section if skipped.}

### Representative diffs

{3–5 inline before/after pairs. For each: document_contents_id (truncated),
baseline field values, experimental field values, classification (Fixed /
Regression / Unrelated), and relevant excerpts from document_text to explain why
the change happened.}

### Re-run the comparison

Save the SQL below to a file and pipe it to `bq query` to reproduce the diff:

<details>
<summary>Diff SQL</summary>

\`\`\`sql
{full comparison SQL from Phase 5b, with all target doc IDs inlined}
\`\`\`

</details>

```bash
bq query \
  --project_id={project} \
  --use_legacy_sql=false \
  --format=json \
  --max_rows=500 \
  < cni_experiment_{experiment_name}_diff.sql
```

## Golden eval accuracy

Report unit and sample separately — they have different baseline accuracy
profiles and a regression in one can be masked by improvement in the other.

**Unit tests** ({n} cases — targeted, specific behaviors):

| Field       | Main   | Branch | Delta |
| ----------- | ------ | ------ | ----- |
| {field_1}   | {acc}% | {acc}% | {±pp} |
| ...         |        |        |       |
| **Total**   | {acc}% | {acc}% | {±pp} |

**Sample tests** ({n} cases — realistic note variety):

| Field       | Main   | Branch | Delta |
| ----------- | ------ | ------ | ----- |
| {field_1}   | {acc}% | {acc}% | {±pp} |
| ...         |        |        |       |
| **Total**   | {acc}% | {acc}% | {±pp} |

Flag any field where branch accuracy drops >2pp with a ⚠. For each flagged
field, check whether it is in scope for the fix. If the regression is on a
field unrelated to the fix (e.g. `dependent_housing_type` dropped while the
fix targets `primary_status`), call that out explicitly so reviewers don't
conflate it with the fix's effect.

## Overall conclusion

{One paragraph: did the fix work (sandbox), did it regress anything (eval), and
what's the recommended next step (ship, iterate, investigate regression).}
````

---

## Phase 8.5: Human review gate

**Do not proceed to Phase 9 until the user confirms they have reviewed the
Google Sheet.**

After completing the report, prompt the user:

> "Before posting to the PR, please review the spot-check sheet to confirm
> the classifications and diffs look correct:
> [{sheet_title}]({sheet_url})
>
> In particular, check:
> - Any **Regression** rows — confirm they are genuine regressions, not
>   annotation errors.
> - A sample of **Fixed** rows — confirm the branch output is actually better,
>   not just different.
> - Any **Unrelated** rows — confirm the change is truly unrelated to the fix
>   and doesn't warrant investigation.
>
> Let me know when you've reviewed and I'll post the sanitized summary to the
> PR."

Wait for explicit confirmation before continuing to Phase 9.

---

## Phase 9: Offer to post to the PR

Ask the user:

> "Would you like me to post this experiment report to the PR for
> `{fix_branch}`?"

If yes, build a **sanitized PR comment** — `pulse-data` is a public repo,
so the PR comment itself must contain **no PII**: no document text, no
`document_contents_id` values, no individual-level examples. Include only
aggregate numbers, the re-runnable SQL query (which contains only SHA256
hashes as document identifiers), and — if Phase 5d was completed — a link to
the private Google Sheet. The sheet itself is access-controlled, so its URL is
safe to post; anyone without data access will receive a 403.

The sanitized comment has this structure:

```markdown
## CNI Experiment: {experiment_name}

**State / Collection**: {STATE_CODE} / {COLLECTION_NAME_UPPER}
**Branches**: `main` vs `{fix_branch}`

### Hypothesis

{expected_change_spec from Phase 1 — paraphrase if it contains specific field
values or examples that could be traced to a real person; keep it abstract}

### Sandbox results ({total_target_docs} target documents)

{fixed_count}/{total_target_docs} showed the expected change.
{regression_count} regressions. {unrelated_count} unrelated changes.

Spot-check sheet (requires data access — contains PII): [{sheet_title}]({sheet_url})
*(Omit this line if Phase 5d was skipped.)*

| Field | Changed | % | Notes |
|-------|---------|---|-------|
| {field_1} | {n} | {%} | {e.g. "all Fixed", "2 regressions"} |
| ...   |        |   |       |

### Golden eval accuracy

**Unit tests** ({n} cases):

| Field | Main | Branch | Delta |
|-------|------|--------|-------|
| {field_1} | {acc}% | {acc}% | {±pp} |
| ... | | | |
| **Total** | {acc}% | {acc}% | {±pp} |

**Sample tests** ({n} cases):

| Field | Main | Branch | Delta |
|-------|------|--------|-------|
| {field_1} | {acc}% | {acc}% | {±pp} |
| ... | | | |
| **Total** | {acc}% | {acc}% | {±pp} |

{⚠ flags for any field with >2pp regression. Note whether flagged fields are
in scope for the fix or unrelated — this helps reviewers interpret the delta.}

### Re-run the comparison

<details>
<summary>Diff SQL</summary>

\`\`\`sql
{full comparison SQL from Phase 5b — safe to include, contains only
SHA256 hashes as document identifiers}
\`\`\`

</details>

### Conclusion

{Same one-paragraph conclusion as the local report, with no document-level
specifics.}

---
🤖 Generated with [Claude Code](https://claude.com/claude-code)
````

First, look up the PR for the fix branch. If there is no PR yet, tell the user
and offer to post the comment once a PR is created (save the sanitized comment
to `/tmp/cni_experiment_{experiment_name}_pr_{YYYYMMDD}.md` so it's ready).

```bash
gh pr view --head {fix_branch} --json number,url
```

If a PR exists, post the comment:

```bash
gh pr comment {pr_number} --body "$(cat /tmp/cni_experiment_{experiment_name}_pr_{YYYYMMDD}.md)"
```

Confirm the comment URL after posting.

---

## Data privacy reminders

- Never query US_ME or US_CA data.
- The diff JSON and the full local report contain PII (document text, case note
  content). Keep them on your local machine only; never paste into Slack,
  Notion, GitHub issues, or any public channel.
- `pulse-data` is a **public** GitHub repo. The PR comment must never
  contain document text or any content that could identify a person. Only
  aggregate counts, percentages, accuracy numbers, and the diff SQL (which
  contains only SHA256 hashes) are safe to post.
- Delete the diff JSON after the report is finalized:
  ```bash
  rm /tmp/cni_experiment_{experiment_name}_diff.json
  ```

---

## Related skills

- [`sample-cni-documents`](../sample-cni-documents/SKILL.md) — Phase 2 target
  set assembly.
- [`configure-cni-source`](../configure-cni-source/SKILL.md) — wire up a new
  state before running experiments on it.
