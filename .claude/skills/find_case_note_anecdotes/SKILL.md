---
name: find_case_note_anecdotes
description:
  Find illustrative anecdotes from supervision case notes for a given topic.
  Pulls a filtered batch of raw case notes from BigQuery (using existing
  document collection configs as the source query), groups by person, and reads
  through them to surface salient examples. Optionally scopes the candidate set
  to "Recidiviz-implicated" people — those who interacted with our tools
  (Workflows opportunity engagement, the Tasks contact-compliance tool, the PSI
  sentencing assistant pre-sentence, or the JII tablet app) — for
  impact-attributed storytelling. Use when the user asks for case note
  anecdotes, real-world examples from case notes, narrative stories from
  supervision data, qualitative evidence for an impact deck, or wants to back up
  an analysis with qualitative evidence.
---

# Skill: Find Case Note Anecdotes

## Overview

This skill standardizes the workflow to surface anecdotes from case notes:
filter raw case notes down to a plausible candidate pool with a BQ query, then
have Claude read through the filtered set and pull out salient examples. It
leans on the source queries already encoded in the document collection YAMLs at
[recidiviz/NOT_FOR_PRODUCTION_USE/documents/store/config/document_collections/](recidiviz/NOT_FOR_PRODUCTION_USE/documents/store/config/document_collections/),
so we get one consistent way to access raw case notes per state.

TODO(#78183): Update path to productionized version of case notes configs
directory.

**Caveats up front:**

- Always treat the output as a starting point that needs human review, not a
  finished artifact.
- Results are anecdotal and not statistically representative. Do not present
  them as such.
- Never query US_ME or US_CA case notes — see CLAUDE.md.

---

## Step 1: Gather inputs from the user

Ask the user (in one message) to write a short request for what kind of anecdote
they're looking for in terms of these three inputs. List the defaults inline and
tell the user they can override any of them in the same reply.

1. **Topic** — What kind of anecdote are they looking for? (e.g., "loss of
   housing", "employer terminating someone for an absence", "successful
   completion of a treatment program"). Get specific enough to translate into
   keywords.

2. **State(s)** — Which state(s)? Currently configured: `US_AZ`, `US_CO`,
   `US_IX`, `US_NC`, `US_ND`, `US_NE`, `US_TN`. **Never** query `US_ME` or
   `US_CA` (per CLAUDE.md).

3. **System Type(s)** Do you want to look at incarceration case notes,
   supervision case notes, or both?

4. **Recidiviz-implicated scope** (optional). Default is **none** (any person
   whose case notes fit the topic). Otherwise, you can specify something like
   "people who were viewed in Workflows for the supervision level downgrade
   opportunity before receiving it" or "people who were registered for the JII
   tablet app before being released to supervision."

   See Step 3.5 for the SQL patterns. Default to no implication scoping unless
   the user mentions impact, attribution, an internal deck, or product-team
   storytelling.

State the following defaults to the user and tell them they can override:

- **How many anecdotes**: 5–10
- **Recency window**: last 1 year
- **Project**: `recidiviz-staging` (mirrors prod for raw data)
- **Exclusions**: none

Tell the user: "For example, you can ask for anecdotes of people in Idaho who
were granted work release after being viewed in Workflows and have secured
stable employment on supervision."

Translate the topic into a 5–15 keyword list before moving on (include variants
and abbreviations — e.g. for housing loss: `evict`, `eviction`, `homeless`,
`kicked out`, `lost housing`, `shelter`, `nowhere to stay`, `couch surf`). Show
the keyword list back to the user for confirmation before running the query —
the keyword list is the load-bearing part of the filter.

### Validate state coverage before proceeding

The skill has **two independent state gates**. Check both before running any
queries; if either fails, stop and tell the user which gate failed and what the
options are.

**Gate 1 — Case-notes config exists for the requested state.** Look for a file
in:

- `recidiviz/NOT_FOR_PRODUCTION_USE/documents/store/config/document_collections/{state}`

Ignore context.yaml documents in that directory.

Without this, the skill has no canonical `document_generation_query` to build on
and can't run at all. Configs are owned by @mayukas and @ageiduschek; if a state
needs a new one, that's a separate workstream. If the user requests a state not
in the configured list (Step 1), stop and offer: (a) switch to a state that has
a config or (b) skip the skill and write a one-off query against any per-state
case notes view they have in mind.

**Gate 2 — Recidiviz-tool footprint exists for the requested state** (only
applies if Step 1.6 specified Recidiviz-implication scoping). Invoke
[find_recidiviz_implicated_people](../find_recidiviz_implicated_people/SKILL.md) and
use its **State-coverage check** under Mode A to confirm the requested state
has nontrivial volume for the implication signal (Workflows funnel, JII
tablet app, Tasks preview, PSI, Insights/Outliers, etc.).

If the requested state has zero (or near-zero) implicated people, tell the
user the state isn't yet on the relevant Recidiviz product so the
implication-scoped search would return nothing. Offer to (a) drop the
implication scoping (see if the case-notes-only search yields anything
useful), (b) switch states to one where the product is live, or (c) confirm
the gap is the answer the user wanted.

---

## Step 2: Find the source query

For each requested state, the case notes source query lives in:

`recidiviz/NOT_FOR_PRODUCTION_USE/documents/store/config/document_collections/{state}/`

Choose which document store(s) to use based on whether the user specified to
reference incarceration case notes, supervision case notes, or both.

Ignore context.yaml files in that directory.

Read the YAML and extract:

- The `document_generation_query` block (this is the canonical "raw case notes
  for this state" query — it produces a `document_text` column plus per-state
  primary key columns like `person_external_id` and contact date).
- The `primary_key_columns` and `additional_metadata_columns` so you know what
  fields are available for filtering and grouping.

### Profile contact types before filtering

Pull a one-shot `GROUP BY contact_type` distribution before the main query. Two
recurring traps to watch for: **auto-generated stub records** (state systems
sometimes write a stub-text note when a structured event fires — treat as a
_signal_, not narrative) and **templated treatment-provider notes** (CBT/DBT/MRT
session-attendance boilerplate that keyword-matches without telling you anything
specific). Restrict the main query to PO/officer narrative types (typically
office visits and home visits) unless the user explicitly wants provider
content.

---

## Step 3: Build the filtered query

Wrap the `document_generation_query` as a CTE, then filter it down.

> **Order of operations (load-bearing).** `REGEXP_CONTAINS`, `LOWER(...)`, and
> `ARRAY_LENGTH(SPLIT(...))` over `document_text` are the expensive parts of
> this query — each one scans the full note body. Apply the cheap filters first
> to slim the row count, then run string parsing only on what survives:
>
> 1. **Select raw case notes** from the YAML's `document_generation_query`.
> 2. **Filter on metadata columns**: `document_update_datetime` (recency),
>    `contact_type` / `note_type` (PO-narrative allowlist).
> 3. **JOIN to the eligible person list** via `state_person_external_id` (only
>    if Step 3.5 implication scoping is in play).
> 4. **Only now** run `REGEXP_CONTAINS` and `ARRAY_LENGTH` to tag keyword
>    matches and substantive-length notes.
> 5. Aggregate per person and apply the `matching_note_count` threshold.

> **Root-entity column is YAML-driven, not hardcoded.** The
> `document_generation_query` emits columns matching the YAML's
> `root_entity_type`. The template below uses `<root_entity_id_col>` as a
> placeholder — substitute the column name for the YAML you're running against:
>
> | YAML `root_entity_type` | Column(s) emitted                                                  | `<root_entity_id_col>` |
> | ----------------------- | ------------------------------------------------------------------ | ---------------------- |
> | `PERSON_EXTERNAL_ID`    | `person_external_id` (STRING) + `person_external_id_type` (STRING) | `person_external_id`   |
> | `PERSON_ID`             | `person_id` (INT64)                                                | `person_id`            |
> | `STAFF_ID`              | `staff_id` (INT64)                                                 | `staff_id`             |
> | `STAFF_EXTERNAL_ID`     | `staff_external_id` (STRING) + `staff_external_id_type` (STRING)   | `staff_external_id`    |
>
> As of writing, all 8 configured supervision case-note / contact-note YAMLs use
> `PERSON_EXTERNAL_ID`, so in practice `<root_entity_id_col>` is
> `person_external_id` today. The `implicated_external_ids` CTE in the template
> below assumes `PERSON_EXTERNAL_ID` — see the inline comment there for what
> changes if the YAML uses a different root entity type. Source of truth for the
> column-name mapping: `DocumentRootEntityIdType` in
> [recidiviz/NOT_FOR_PRODUCTION_USE/documents/store/document_collection_config.py](recidiviz/NOT_FOR_PRODUCTION_USE/documents/store/document_collection_config.py).

The default filter recipe (with optional implication scoping wired in):

```sql
WITH raw_notes AS (
  -- Contents of document_generation_query, with {project_id} substituted,
  -- plus a human-readable `note_id` column constructed as:
  --   CONCAT(state_code, '|', <root_entity_id_col>, '|', <primary_key_columns>)
  -- (e.g. for US_CO: 'US_CO|OFFENDERID|CONTACTDATE|CONTACTTIME|CONTACTTYPE')
  ...
),

-- OPTIONAL: only include this CTE if Step 3.5 implication scoping is requested.
-- Substitute the appropriate per-product filter from Step 3.5 (Workflows
-- funnel, Tasks preview, PSI bridge, JII tablet app, Insights/Outliers, etc.).
-- The output column name must match the YAML's `<root_entity_id_col>` so it
-- joins directly to raw_notes. The CTE body shown below assumes the YAML uses
-- `root_entity_type: PERSON_EXTERNAL_ID` and bridges from the Recidiviz
-- internal `person_id` to the state-specific external_id via
-- `state_person_external_id`. For a YAML with `root_entity_type: PERSON_ID`,
-- the bridge collapses (just `SELECT DISTINCT person_id FROM funnel WHERE ...`).
-- For `STAFF_ID` / `STAFF_EXTERNAL_ID`, the implication source itself changes
-- (notes are about staff, not JIIs) — adapt accordingly.
implicated_external_ids AS (
  SELECT DISTINCT pei.external_id AS <root_entity_id_col>
  FROM `{project_id}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` funnel
  INNER JOIN `{project_id}.normalized_state.state_person_external_id` pei
    ON pei.state_code = funnel.state_code AND pei.person_id = funnel.person_id
  WHERE funnel.task_type = '<task_type>'
    AND (funnel.surfaced OR funnel.viewed)
    AND funnel.task_completed
    AND pei.id_type = '<id_type from case-notes YAML>'  -- e.g. 'US_IX_DOC'
),

candidate_notes AS (
  SELECT
    rn.<root_entity_id_col>,
    rn.note_id,
    rn.document_update_datetime AS note_datetime,
    rn.contact_type,
    rn.document_text AS note_text
  FROM raw_notes rn
  -- Drop this INNER JOIN line entirely if Step 3.5 scoping is NOT requested.
  INNER JOIN implicated_external_ids USING (<root_entity_id_col>)
  WHERE rn.document_update_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
    AND rn.contact_type IN ()  -- Filter to relevant contact types
),

tagged_notes AS (
  SELECT
    *,
    REGEXP_CONTAINS(LOWER(note_text), r'(evict|homeless|kicked out|lost housing|shelter|...)') AS is_keyword_match,
    ARRAY_LENGTH(SPLIT(note_text, ' ')) >= 50 AS is_substantive
  FROM candidate_notes
)

SELECT
  <root_entity_id_col>,
  COUNTIF(is_keyword_match AND is_substantive) AS matching_note_count,
  COUNT(*) AS total_note_count,
  MIN(note_datetime) AS first_note_datetime,
  MAX(note_datetime) AS last_note_datetime,
  ARRAY_AGG(
    STRUCT(note_id, note_datetime, contact_type, note_text, is_keyword_match)
    ORDER BY note_datetime
  ) AS notes
FROM tagged_notes
GROUP BY <root_entity_id_col>
HAVING matching_note_count >= 1   -- bump higher (e.g., >= 3) if the topic is noisy
ORDER BY matching_note_count DESC
LIMIT 200
```

**ARRAY_AGG of STRUCTs (not STRING_AGG into one blob)** — each note is
addressable by `note_id` so the model can cite it and the reviewer can `jq`
straight to it.

Notes on the filter:

- **Project**: default to `recidiviz-staging`. Confirm with the user if they
  want prod data.
- **Sample size**: aim for a final batch of roughly 100–300 _people_ (~5K–30K
  notes after grouping). If the keyword filter returns too many people, raise
  the `matching_note_count` threshold or tighten keywords. If it returns too
  few, broaden keywords or extend the recency window.
- **Word count**: `>= 50` is a reasonable default; lower it for narrative-light
  states.
- **Keyword filter**: use `REGEXP_CONTAINS(LOWER(...), r'...')` with `|` to OR
  keywords. Watch out for substrings that match too aggressively (e.g.,
  `homeless` will not match `home`, but `home` would match `homeland`).
- **State code**: never query US_ME or US_CA. If the user requested multiple
  states, run one query per state — the source queries differ per state and
  shouldn't be UNIONed blindly.

Show the filter recipe (keywords, recency, word count, person count threshold,
contact-type allowlist) back to the user before running. Surface the row count
of the unfiltered keyword match first as a sanity check.

---

## Step 3.5: Filter to Recidiviz-implicated people (optional)

Use this step only if the user answered Step 1.6 with anything other than
"None." It supplies the **`implicated_external_ids` CTE** that gets wired into
the Step 3 query as an `INNER JOIN` target inside `candidate_notes`, so every
person in the final anecdote pool has documented contact with our products in
some way.

**Invoke
[find_recidiviz_implicated_people](../find_recidiviz_implicated_people/SKILL.md) in
cohort mode** with the user's implication description and the relevant
state(s). That skill is the source of truth for per-product table catalogs
(Workflows funnel, Tasks preview vs. Workflows-rendered-on-Tasks-URL, PSI
bridge, JII tablet app, Insights/Outliers officer flag) and the
`person_id` ↔ `external_id` bridge through `state_person_external_id`.

The CTE it emits must, for this skill's purposes:

- Be named `implicated_external_ids` and emit a column that joins to Step 3's
  `<root_entity_id_col>` (for the configured supervision-notes YAMLs, all
  `PERSON_EXTERNAL_ID`, that means a column named `person_external_id` filtered
  to the right `id_type` — `'US_IX_DOC'`, `'US_TN_DOC'`, `'US_CO_OFFENDERID'`,
  etc.).
- Optionally emit a per-person `first_engagement_date` (or
  `task_completed_date`) column when the **temporal ordering rule** below
  applies — Step 3 then enforces `note_datetime >= <that_date>` so cited notes
  postdate the tool action.

The same pattern works when the structured signal is _not_ Recidiviz-tool
engagement — e.g., a state-system event row (AET credit award, sentence
modification, custody downgrade). Substitute that event for the tool
engagement in the cohort-mode CTE; the output shape stays the same.

### Temporal ordering rule (load-bearing for attribution)

**Notes used to support an outcome / attribution claim must come AFTER the tool
action being attributed.** A claim like "the Recidiviz tool helped this person
stabilize on supervision" requires citing notes that postdate the tool
surface/view/grant — otherwise the tool can't have caused the outcome.

Concretely:

- **Outcome claims** (thriving, employment found, housing stable, sober,
  successfully discharged, etc.) → cited `note_datetime` must be
  `>= tool_first_engagement_date` (or, more conservatively,
  `>= task_completed_date` for grant-attributed claims).
- **Background / context claims** (the person was struggling with X before the
  tool engaged them) → may cite notes that predate the tool action; just label
  them as background, not attribution.
- **Concurrent claims** (the tool surfaced them at the same time as a PO
  contact) → fine to cite same-day notes; flag the concurrence explicitly so the
  reviewer can judge whether to call it attribution.

Step 5's citation rule is amended accordingly: when a `[note_id=...]` citation
is used to back an outcome claim under implication scoping, the model must
verify the note's `note_datetime` is on or after the relevant tool-action date
in the JSON record before citing.

---

## Step 4: Run the query

Run via the BigQuery MCP server if available, otherwise via the `bq` CLI:

```bash
bq query --project_id=recidiviz-staging --use_legacy_sql=false --format=json --max_rows=300 "<query>"
```

For longer queries, write the SQL to a temp file and pipe it in:

```bash
bq query --project_id=recidiviz-staging --use_legacy_sql=false --format=json --max_rows=300 < /tmp/case_note_anecdote_query.sql
```

If `bq` returns `ReauthUnattendedError` or "Reauthentication required", the
user's gcloud session has expired — non-interactive shells can't answer the 2FA
challenge. Stop and ask the user to run:

```bash
gcloud auth login --update-adc
```

Then re-run the same query.

For result sets larger than a few rows, stream the output to a local JSON file
in `/tmp/` so subsequent reads of specific rows are cheap:

```bash
bq query --project_id=recidiviz-staging --use_legacy_sql=false \
  --format=json --max_rows=300 \
  < /tmp/case_note_anecdote_query.sql \
  > /tmp/case_note_anecdotes_{topic_slug}_{YYYYMMDD}.json
```

This JSON file is **internal working memory for the model only — not a
deliverable for the reviewer.** The model reads it to evaluate candidates and
pick the anecdote(s); after the `.md` report is finalized in Step 5, **delete
the JSON** (everything that mattered is now either in the report or re-fetchable
from BQ via the verification SQL in the report). Do not persist results to a
BigQuery scratch table either.

`bq --format=json` serializes booleans as strings, so when reading the JSON with
`jq` during candidate evaluation, compare with `== "true"` rather than relying
on jq truthiness.

---

## Step 5: Read through and pick anecdotes

Read the JSON row by row (one row = one person, with their notes in the `notes`
array). For each candidate, evaluate:

- **Is the topic actually about this person?** Keyword matches produce false
  positives (e.g., a note that says "client is _not_ homeless" matched
  `homeless`). Skip false positives silently.
- **Is there a coherent narrative?** A single ambiguous mention is rarely a good
  anecdote. Look for multiple notes that together tell a story (escalation,
  resolution, intervention).
- **Is it specific?** Vague notes ("client is doing fine") aren't useful even if
  they technically match. Prefer notes with concrete details (dates, names of
  programs, direct quotes from the client).
- **Does it match what the user is looking for?** If the queries don't return
  results that match the type of anecdote the user is looking for, be
  transparent about that. You can still return similar anecdotes, but be up
  front that they don't exactly align with the experiences the user was hoping
  to find.

### Citation requirement

Every statement in an anecdote must be backed by an inline citation to a
specific note. Use the `note_id` field from the JSON as the citation token, e.g.
`[note_id=US_CO|99999999|2026-01-29|14:30|OV2]`. Never paraphrase a detail
without citing the note it came from — without a `note_id`, the reviewer cannot
verify the statement against the source.

**Temporal validity for attribution claims.** Under implication scoping (Step
3.5 active), any cited note used to back an _outcome_ statement — that the tool
contributed to the person's trajectory — must have a `note_datetime` on or after
the relevant tool-action date. A "thriving on supervision" claim cited to a note
from before the tool surfaced/viewed the person isn't attribution; it's
pre-existing state. See Step 3.5's "Temporal ordering rule." Notes that predate
the tool action are fine for background context, but must be labeled as such,
not as outcome evidence.

### Output one artifact (the `.md` report)

Write `case_note_anecdotes_{topic_slug}_{YYYYMMDD}.md` to `/tmp/`. This is the
only deliverable for the reviewer.

For each selected anecdote, the report should include:

- Header with `person_external_id`, `id_type`, `state_code`, date range,
  `matching_note_count`.
- One-paragraph explanation of reasoning for selecting this person, with
  `[note_id=...]` citations inline after each reason. Lean on couching your
  reasoning in caveats rather than declaring a narrative with confidence. It's
  more important to accurately represent the shortcomings of case notes in
  representing the narrative the user is searching for rather than
  overconfidently asserting a narrative with shaky evidence.
- 1–3 short direct quotes pulled from cited notes, attributed by `note_id` and
  date.
- A **Cited notes** section: one collapsible `<details>` block per cited
  `note_id`, containing the verbatim `note_text`:

  ```markdown
  <details>
  <summary><code>US_CO|99999999|2026-01-29|14:30|OV2</code> — 2026-01-29 OV2</summary>

  <verbatim note_text>

  </details>
  ```

  This lets the reviewer expand any cited note and read the full source without
  leaving the report.

The reviewer can run the SQL queries in the "Reviewer notes / lookups" section
of the report to see cited evidence. They never need the JSON.

**Caveat to flag in the report:** the model picked the anecdote based on data
fetched in Step 4. If the source case-notes table is updated between Step 4 and
the reviewer running the verification SQL, the reviewer might see slightly
different content. Case notes don't change much day-to-day, but flag this so the
reviewer knows what to expect.

### Verifying Recidiviz-implication data points (when Step 3.5 is active)

Add a section to the report titled **Recidiviz-implication data points**: one
labeled SQL block per data point, scoped to the specific
`(state_code, person_id)`, with a one-sentence claim label and an
expected-result note. Same hard rule as case-note `[note_id=...]` citations —
no claim without a verifiable snippet.

**Invoke
[find_recidiviz_implicated_people](../find_recidiviz_implicated_people/SKILL.md) in
individual mode** with the person's `(state_code, person_id)` to get the
right drill-down SQL for each claim shape (Workflows engagement+grant,
JII registration / login count, Tasks-tool preview, PSI case view, officer
flagged as outlier, etc.). The skill's per-product subsections include
single-person drill-down templates that drop straight into the report.

To bridge case-notes `external_id` → `person_id` for the verification SQL,
query `normalized_state.state_person_external_id` filtered by the YAML's
`id_type`.

---

### Reviewer notes / lookups (verification SQL in the report)

The report's **Reviewer notes / lookups** section must include the following SQL
queries against the YAML's source table for the relevant state. All queries
return rows from BQ, not from a local cache — the reviewer never needs the JSON.

1. **All cited notes, in date order.** Single query using `IN (...)` over the
   third pipe-segment of each cited `note_id`. Example for US_NE (single-column
   PK `contactDashboardId`):

   ```sql
   SELECT *
   FROM `recidiviz-staging.us_ne_raw_data_up_to_date_views.PIMSContactDashboard_latest`
   WHERE inmateNumber = '99999999'
     AND CAST(contactDashboardId AS STRING) IN ('1111111', '2222222', '3333333')
   ORDER BY dateOfContact
   ```

   Example for US_CO (composite PK using a tuple `IN (...)`):

   ```sql
   SELECT *
   FROM `recidiviz-staging.us_co_raw_data_up_to_date_views.eomis_supervisioncontact_latest`
   WHERE OFFENDERID = '99999999'
     AND (CAST(CONTACTDATE AS STRING), CONTACTTIME, CONTACTTYPE) IN (
       ('2026-01-29', '14:30', 'OV2'),
       ('2026-02-03', '09:15', 'HV2')
     )
   ORDER BY CAST(CONTACTDATE AS TIMESTAMP), CONTACTTIME
   ```

2. **All notes for the primary subject** (so the reviewer can read uncited
   surrounding context the model didn't quote). Same source table, just the
   person filter:

   ```sql
   SELECT *
   FROM `recidiviz-staging.us_ne_raw_data_up_to_date_views.PIMSContactDashboard_latest`
   WHERE inmateNumber = '99999999'
   ORDER BY dateOfContact
   ```

3. **Notes for any backup candidate** — same shape as #2, parameterized by
   `external_id`. List the backup candidates' external_ids in the report's
   "Backup candidates" section so the reviewer can swap them in directly.

---

## Step 6: Suggest saving the anecdotes

After producing the list, remind the user:

If you find a good anecdote, add it to the
[Case Notes Anecdotes Library](https://docs.google.com/spreadsheets/d/1Ss9aworBhc4mVK0vu7phggSnOkR1rTSJVo9tUUOKNWU/edit?gid=2051863877#gid=2051863877)
Google sheet. Remember that these contain PII. Do not share case notes anecdotes
anywhere we cannot share PII.

---

## Step 7: Clean up the JSON; prompt before deleting the report

Delete the Step 4 JSON immediately after the `.md` is finalized — internal
working memory only, and PII-dense. Don't delete the `.md` without explicit user
confirmation; `/tmp/` cleans itself up eventually.

---

## Data privacy reminders

- **Never query US_ME or US_CA data**, even though configs exist.
- **Case notes contain PII embedded in the note body** — person names, officer
  names, addresses, phone numbers, MH/SUD diagnoses, third-party names, and
  facility names. Auto-generated stub notes can be especially
  structured-PII-heavy. The `.md` report stays on the local machine; do not
  paste full notes into Slack, GitHub, Notion, etc. without redaction.
- **When summarizing externally**, paraphrase and replace identifying details
  (names, addresses, employers, phone numbers, specific dates) with role
  descriptors ("the client", "the supervising officer"). When in doubt, share
  via 1:1 DM rather than a wider channel.

## Related

- [recidiviz/NOT_FOR_PRODUCTION_USE/documents/CLAUDE.md](recidiviz/NOT_FOR_PRODUCTION_USE/documents/CLAUDE.md)
  — full architecture for the document store and extraction packages.
- [CJ's case notes narrative method](https://docs.google.com/document/d/13Mlc5VCWG-Z7_7LXXkDwThrlt2g02ukC8ZbkPCPCjtk/edit?tab=t.0)
  — the manual-review precursor to this skill.
- [CJ's `[US_MI] Case Note Narrative` Colab](https://colab.research.google.com/drive/1adTK0DIiVzxSv2H8Xn46QOTZ9ukRuX-b?usp=sharing)
  — worked example of the manual-review method on US_MI: filters candidates by
  system-of-record achievement signals (supervision / custody downgrades,
  completed programs, low compartment-session count) before keyword filtering,
  then deep-dives a single person with notes joined to compartment + custody
  level at note date plus officer, program, and violation history.
