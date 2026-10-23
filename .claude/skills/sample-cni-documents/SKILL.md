---
name: sample-cni-documents
description: >
  Sample documents from the CNI document store for annotation, prompt
  experimentation, or golden eval set seeding. Supports filtering by keywords,
  length, metadata columns, semantic content, or extracted field values. Outputs
  a private Google Sheet with person_id, document_contents_id, and document
  text. Use when you need a structured dataset of case notes matching specific
  criteria — for annotation, prompt tuning, or eval sampling. Distinct from
  find_case_note_anecdotes, which produces a narrative .md report; this skill
  produces a structured sheet.
---

# Skill: Sample CNI Documents

## Overview

This skill pulls a structured sample of documents from the CNI document store.
The document store holds the cleaned, normalized text the extraction LLM
actually sees — not the raw source data. The output is a private Google Sheet
keyed by `document_contents_id` that you can hand directly to an annotator or
feed into a prompt experiment.

Two independent decisions shape how you sample:

1. **Starting pool** — document store (Step 3) or extraction results (Step 3.7).
   Use the document store when you have no constraint on what the extractor has
   already produced. Use the extraction results table when you want to target
   documents where the extractor already yielded a specific field value —
   particularly useful when investigating a suspected error pattern (e.g.,
   "find notes where the model tagged `search_status = 'searching'` but the
   text is about job search").

2. **Selection within the pool** — SQL filters alone, or SQL filters followed
   by sample-then-read (Step 3.5). Apply SQL first to narrow cheaply; add
   sample-then-read on top when the remaining criterion is semantic and can't
   be expressed in a WHERE clause.

These decisions are independent and composable: SQL-filter the document store,
SQL-filter the extraction results, sample-then-read from the document store, or
SQL-filter the extraction results and then sample-then-read to confirm errors —
whatever combination fits the task.

**Caveats:**

- Never query US_ME or US_CA data — see CLAUDE.md.
- Document text contains PII. Keep the output CSV on a local machine or in an
  approved storage location; do not paste document text into Slack, GitHub, or
  Notion.

---

## Step 1: Gather inputs

Ask the user (in one message) for the following. State the defaults inline and
tell the user they can override any of them.

1. **State code** — e.g., `US_TN`. Never `US_ME` or `US_CA`.

2. **Extractor collection** — which extractor to sample from (e.g.,
   `case_note_employment_info`). If the user isn't sure, list the directories in
   `recidiviz/documents/config/extractor_collections/` filtered by state.

3. **Sample size** — how many documents. Default: **50**.

4. **What you're looking for** — describe the target documents in plain
   language. From this, determine the two decisions above:
   - Does the criterion involve a specific extractor output value (e.g., "notes
     where the model said `search_status = 'searching'`")? → extraction results
     as starting pool (Step 3.7); otherwise use the document store (Step 3).
   - Can the full criterion be expressed in a WHERE clause? → SQL filters alone;
     otherwise layer sample-then-read on top (Step 3.5).

5. **Filter criteria** — keywords, contact types, date range, length range,
   extracted field values, or semantic description.

6. **Project** — default `recidiviz-staging`.

---

## Step 2: Look up the collection config

Read the extractor config YAML at:

```
recidiviz/documents/config/extractor_collections/{extractor_collection}/{state_code_lower}/extractor.yaml
```

Extract:

- `input_document_collection_name` — the doc store collection name in
  SCREAMING_SNAKE_CASE (e.g., `SUPERVISION_CASE_NOTES`). Convert to lowercase
  for BQ table names.
- `document_metadata_filter_query_template` — the SQL template the extractor
  uses to determine which documents it processes. This must be incorporated into
  the sampling query (Step 3) so you only sample from documents the extractor
  can actually read from.
- `state_code` (from the directory path) — confirm it matches the user's input.

The doc store BQ tables for this collection are:

| Role     | Dataset                                      | Table                                       |
| -------- | -------------------------------------------- | ------------------------------------------- |
| Metadata | `{state_code_lower}_document_store_metadata` | `{collection_name_lower}`                   |
| Text     | `{state_code_lower}_document_contents`       | `{collection_name_lower}_document_contents` |

**Example** for `US_IX` + `SUPERVISION_CASE_NOTES`:

- Metadata: `us_ix_document_store_metadata.supervision_case_notes`
- Contents: `us_ix_document_contents.supervision_case_notes_document_contents`

**Render the filter template:** substitute
`{input_document_collection_metadata_address}` with the fully-qualified metadata
table address:
`{project}.{state_code_lower}_document_store_metadata.{collection_name_lower}`

So for the US_CO housing extractor the rendered filter becomes:

```sql
SELECT DISTINCT document_contents_id
FROM `recidiviz-staging.us_co_document_store_metadata.supervision_contact_notes`
WHERE document_contents_id IS NOT NULL
  AND contact_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY)
  AND contact_type NOT IN ('AT', 'AET', 'RA1', 'HRF', 'TCJ', 'TCK', 'PRO')
```

Inspect the metadata table schema to see exactly which columns are present — not
all collections have the same set of framework columns (e.g., some have
`person_external_id` but not `person_id`):

```bash
bq show --schema --format=prettyjson \
  recidiviz-staging:{state_code_lower}_document_store_metadata.{collection_name_lower}
```

Include only columns that are actually present in the table. Collection-specific
columns (e.g., `contact_type`, `note_date`) are included in the sheet output.

---

## Step 3: Build and run the query (document store as starting pool)

The query restricts to the extractor-eligible documents first (the rendered
`document_metadata_filter_query_template` from Step 2), then deduplicates to one
row per unique `document_contents_id`, and joins to the contents table for the
actual text. **Always apply the extractor's filter — sampling outside it means
sampling documents the extractor can never process.**

```sql
WITH eligible_documents AS (
  -- Rendered document_metadata_filter_query_template from extractor.yaml:
  -- substitute {input_document_collection_metadata_address} with
  -- {project}.{state_code_lower}_document_store_metadata.{collection_name_lower}
  {rendered_filter_query}
),
deduped_metadata AS (
  SELECT *
  FROM `{project}.{state_code_lower}_document_store_metadata.{collection_name_lower}`
  WHERE document_contents_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY document_contents_id
    ORDER BY row_create_datetime DESC
  ) = 1
),
joined AS (
  SELECT
    m.person_id,
    m.person_external_id,
    m.document_contents_id,
    c.document_text,
    c.document_length_bytes,
    m.document_update_datetime,
    -- include collection-specific metadata columns (e.g. contact_type, note_date)
    {collection_specific_columns}
  FROM eligible_documents e
  JOIN deduped_metadata m USING (document_contents_id)
  JOIN `{project}.{state_code_lower}_document_contents.{collection_name_lower}_document_contents` c
    USING (document_contents_id)
  WHERE {filter_clause}
)
SELECT *
FROM joined
ORDER BY RAND()
LIMIT {sample_size}
```

**Filter clause options — apply cheap filters before string scans:**

| Criterion            | SQL                                                            |
| -------------------- | -------------------------------------------------------------- |
| No additional filter | `TRUE`                                                         |
| Keyword match        | `REGEXP_CONTAINS(LOWER(document_text), r'(term1\|term2\|...')` |
| Min length           | `document_length_bytes >= {n}`                                 |
| Max length           | `document_length_bytes <= {n}`                                 |
| Contact type         | `contact_type IN ('OV', 'HV', ...)`                            |
| Date range           | `document_update_datetime >= TIMESTAMP('{start_date}')`        |

The extractor filter in `eligible_documents` already handles the main
eligibility gates (recency, excluded contact types, etc.). The `{filter_clause}`
in `joined` adds any additional criteria the user specified on top of that.
Apply metadata column filters before `REGEXP_CONTAINS` so the regex only runs on
rows that survive the cheap filters.

**Before running the full query**, run a COUNT to confirm the eligible +
additional filter returns enough rows. Use
`COUNT(DISTINCT document_contents_id)` so the pre-check matches the
deduplication the main query applies via `QUALIFY ROW_NUMBER()`:

```sql
WITH eligible_documents AS (
  {rendered_filter_query}
)
SELECT COUNT(DISTINCT m.document_contents_id) AS matching_docs
FROM eligible_documents e
JOIN `{project}.{state_code_lower}_document_store_metadata.{collection_name_lower}` m
  USING (document_contents_id)
WHERE {filter_clause}
```

If there are fewer rows than `sample_size`, report this to the user and ask
whether to relax the additional filters or accept a smaller sample.

Show the filter plan (extractor eligibility gates, plus any additional
keywords/date range/contact types/length bounds) to the user before running the
main query.

---

## Step 3.5: Sample-then-read (optional — layer on top of either starting pool)

Add this step when the remaining criterion after SQL filtering is semantic and
can't be expressed in a WHERE clause. Examples of criteria that need it:

- "Notes where employment status is ambiguous"
- "Notes the model might hallucinate a plausible-but-wrong detail from"
- "Notes with a clear before-and-after employment narrative"
- "Notes where housing instability is implied but never stated directly"
- "Notes where the extracted `search_status = 'searching'` doesn't match the
  actual document content" (layered on top of Step 3.7)

**Sub-steps:**

1. **Pull a candidate pool.** Run Step 3 or Step 3.7's query with a loose
   additional filter (e.g., keyword-only or `TRUE`) and a limit of 3–5× the
   target sample size. Keep results in memory or save to a temp JSON file in
   `/tmp/` for iteration.

2. **Read and select.** Read each candidate. Evaluate it against the user's
   semantic description. Record the `document_contents_id` of matching
   documents. Keep a running count of candidates read and matches found.

3. **Iterate if needed.** If the first batch doesn't yield enough matches, run
   the BQ query again with `ORDER BY RAND()` for a fresh batch and continue
   reading until the target count is met. Track seen `document_contents_id`s
   across batches and skip any that appear again — small pools can repeat
   documents across `RAND()` pulls.

4. **Assemble the final sample.** Either fetch full rows for matched IDs:
   ```sql
   SELECT m.person_id, m.person_external_id, m.document_contents_id,
          c.document_text, c.document_length_bytes, m.document_update_datetime,
          {collection_specific_columns}
   FROM `{project}.{state_code_lower}_document_store_metadata.{collection_name_lower}` m
   JOIN `{project}.{state_code_lower}_document_contents.{collection_name_lower}_document_contents` c
     USING (document_contents_id)
   WHERE m.document_contents_id IN ({matched_ids_list})
   QUALIFY ROW_NUMBER() OVER (PARTITION BY m.document_contents_id ORDER BY m.row_create_datetime DESC) = 1
   ```
   Or assemble from the JSON already in memory.

For each candidate, evaluate:

- Does it clearly exhibit the target quality?
- Is it substantive (not a one-line stub)?
- Is it a false positive (mentions the topic but doesn't exhibit the quality)?
  Skip false positives silently.

Report how many candidates were evaluated and what selection rate you observed.

---

## Step 3.7: Using extraction results as the starting pool (optional)

Use this when your criterion involves a specific extractor output value —
especially when investigating a suspected error pattern. Starting here guarantees
every document in the pool has been extracted, and you already have the
extractor's answer to evaluate. Documents that haven't been extracted yet are
automatically excluded.

Check the extraction results table schema:
```bash
bq show --schema --format=prettyjson \
  recidiviz-staging:{state_code_lower}_document_extraction_results.{collection_name_lower}
```

Verify enough rows exist with the target field value before pulling:
```sql
SELECT COUNT(*) AS n
FROM `{project}.{state_code_lower}_document_extraction_results.{collection_name_lower}`
WHERE {target_field} = '{target_value}'
```

Then build the pool, joining to metadata and document text. Apply any additional
SQL filters (metadata columns, keywords, length) in the WHERE clause, and set
the limit to 3–5× the target if you'll layer sample-then-read on top:

```sql
SELECT
  e.document_contents_id,
  e.{target_field},
  e.document_update_datetime,
  m.person_external_id,
  m.contact_date,
  m.contact_type,
  {collection_specific_columns},
  c.document_text,
  c.document_length_bytes
FROM `{project}.{state_code_lower}_document_extraction_results.{collection_name_lower}` e
JOIN `{project}.{state_code_lower}_document_store_metadata.{collection_name_lower}` m
  USING (document_contents_id)
JOIN `{project}.{state_code_lower}_document_contents.{collection_name_lower}_document_contents` c
  USING (document_contents_id)
WHERE e.{target_field} = '{target_value}'
  AND {additional_filter_clause}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.document_contents_id ORDER BY m.row_create_datetime DESC
) = 1
ORDER BY RAND()
LIMIT {pool_size}
```

If any selection criterion is semantic (e.g., confirming the extracted value is
actually wrong given the document text), apply sample-then-read (Step 3.5) after
this query.

---

## Step 4: Write results to a Google Sheet

The Google Sheets MCP requires Google Drive access. If you encounter an auth
error, ask the user to run:

```bash
gcloud auth login --enable-gdrive-access --update-adc
```

Create a private Google Sheet titled
`CNI Sample: {state_code} {extractor_collection} ({YYYY-MM-DD})` and write one
row per sampled document. Use the Google Sheets MCP to create and populate it;
do not write a local CSV file.

**Columns (in order):**

| Column                      | Source                                             |
| --------------------------- | -------------------------------------------------- |
| `person_id`                 | metadata table                                     |
| `person_external_id`        | metadata table                                     |
| `document_contents_id`      | metadata / contents                                |
| `document_length_bytes`     | contents table                                     |
| `document_update_datetime`  | metadata table                                     |
| Collection-specific columns | metadata table (e.g., `contact_type`, `note_date`) |
| `document_text`             | contents table — last column                       |

**Privacy:** the sheet contains PII. Create it as a private sheet, visible only
to the authenticated Google account. Do not add viewers or change sharing
settings unless the user explicitly asks.

**README tab**

Add a second sheet tab named `README` (place it first, at index 0) documenting
how the sample was generated. Use a two-column key/value layout. Include:

- **Title and goal** — the sheet title and one sentence on what the sample is
  for (from the user's description of what they're looking for).
- **Parameters** — state, extractor collection, document collection, sample size
  requested vs. delivered, generated date, project.
- **Starting pool** — which table was used (document store or extraction
  results), the filter applied, total rows matching that filter, and how many
  were pulled into the candidate pool.
- **Selection** — method (SQL-only or sample-then-read), the specific filter
  clause and/or semantic criterion used, candidates evaluated, selected count,
  selection rate.
- **What this sample represents** — one sentence on what the sample does and
  does not represent (e.g., "not a random sample of all CO housing extractions —
  specifically documents where job search language drove the conflation").

The README is the sheet's provenance record: anyone opening it later should be
able to reconstruct exactly what query and reading pass produced the rows.

Report to the user:

- Row count written
- Starting pool used (document store or extraction results) and filters applied
- Number of candidates evaluated (if sample-then-read was used)
- Sheet URL

---

## Data privacy reminders

- Never query US_ME or US_CA data.
- The Google Sheet contains PII embedded in `document_text` (person names,
  officer names, addresses, diagnoses, third-party names). Keep it private. Do
  not paste `document_text` values into Slack, GitHub, Notion, or any unapproved
  channel.
- When sharing the sheet with annotators, confirm they have authorization to
  access the state's data before granting access.

---

## Related skills

- [`find_case_note_anecdotes`](../find_case_note_anecdotes/SKILL.md) — produces
  a narrative `.md` report with inline citations for reports and presentations.
  Use this instead when you want illustrative examples to tell a story, not a
  data CSV.
- [`configure-cni-source`](../configure-cni-source/SKILL.md) — wire up a new
  state's document source and extractor config before running this skill on a
  new state.
- [`run-cni-experiment`](../run-cni-experiment/SKILL.md) — uses this skill in
  Phase 2 to assemble a target set of documents for a controlled prompt/config
  experiment.
