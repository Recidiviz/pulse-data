---
name: configure-cni-source
description:
  Configure a new document source for the Case Note Insights (CNI) pipeline —
  wiring both a doc store and extractor configs. Use when adding a new document
  collection for a new state or a new document system within an existing state.
---

# Configure CNI Source

Guide for adding a new document collection source to the Case Note Insights
(CNI) pipeline. CNI uses LLMs to extract structured information (employment,
housing, payment, etc.) from documents such as case notes.

A "source" is a state + document system combination — for example, Tennessee
TOMIS case notes or Arizona supervision contact notes. This could be:

- A **new state**: the state has no CNI document collections yet
- A **new system within an existing state**: the state already has CNI
  collections for supervision case notes, but you want to add facilities case
  notes

## When to use

Use this skill when wiring a new document collection to the CNI pipeline —
including both creating the doc store config and configuring one or more
extractor collections to process those documents.

## Before starting — ask the user for these inputs

Prompt for each input sequentially. Ask one question, wait for the answer, then
ask the next. Do not infer any of these.

**Step A — State code** Ask: "What is the state code? (e.g. `US_TN`)"

**Step B — Extractor collections** Ask: "Which extractor collections should be
configured for this source? (e.g. `case_note_employment_info`,
`case_note_housing_info`, `case_note_payment_info`)" — list the existing
collections by checking `recidiviz/documents/config/extractor_collections/` and
present the options.

> **Note:** This skill only wires an existing extractor collection to a new
> doc store. Creating a brand-new extractor collection type (new prompts, new
> output schema) is a separate process that must be completed first. If the
> user needs a collection that doesn't exist yet, flag it and pause — don't
> proceed until the new collection has been created.

**Step C — State agency name** Ask: "What is the full official name of the state
agency?" — used in the extractor prompt.

**Step D — Document orientation** Ask: "How should we describe these documents
to the model? (1–2 sentences about the source system and who writes them)"

**Step E — Golden eval sheet URLs** Ask: "Do you have golden eval Google Sheet
URLs for each collection? Share whichever you have; I'll use a placeholder for
any not yet ready."

**Step F — Raw data tables** After the state code is confirmed, read the raw
data config at `recidiviz/ingest/direct/regions/{state_code_lower}/raw_data/` to
identify candidate tables that likely hold the documents. Present your findings
and ask the user to confirm which table(s) to use and which columns map to the
document text and any useful metadata. Do not write any query until the user
confirms.

> **Note:** All doc stores and extractors created so far are for supervision
> case notes. Facilities case notes may or may not come from a different source
> — sometimes supervision and facilities notes live in the same raw table and
> are distinguished by a filter column; other times they come from entirely
> separate tables. Clarify the scope with the user before identifying candidate
> tables.

**Step G — Document contents query** Draft the BQ SQL that produces
`(person_external_id, person_external_id_type, document_text, document_update_datetime)`
based on the confirmed tables and columns. Show the draft to the user and ask
them to approve it before writing any files. Ask the user to confirm with the
implementation engineer for that state that the raw data table selection and
query logic are correct before you write any files.

**Step H — Metadata columns** Confirm which metadata columns (if any) to expose
alongside the document text. Do **not** add a date column if the datetime is
already captured by `document_update_datetime` — that column is a standard field
in every metadata table and can be used directly in the extractor filter as
`DATE(document_update_datetime)`.

## Files to create or modify

### 1. Doc store — `recidiviz/documents/config/document_collections/{state_code_lower}/{collection_name}.yaml`

Create this file. Use existing states as reference (e.g.
`recidiviz/documents/config/document_collections/us_az/supervision_case_notes.yaml`).

Required fields:

- `# yaml-language-server: $schema=./../../../store/yaml_schema/schema.json`
  pragma at the top
- `root_entity_id_type: person_external_id`
- `metadata_columns` — only metadata genuinely useful for filtering or
  downstream analysis. Do **not** add a redundant date column when
  `document_update_datetime` already captures it.
- `query` — the BQ SQL that produces the document rows. Read the raw table
  schemas before writing; do not guess column names.

### 2. Extractors — one per collection

These steps assume each extractor collection already exists. If a collection
directory doesn't exist under `extractor_collections/`, it must be created as a
separate effort before continuing here.

For each collection confirmed in Step B, create:
`recidiviz/documents/config/extractor_collections/{collection}/{state_code_lower}/extractor.yaml`

Required fields in each:

```yaml
extractor_collection_name: CASE_NOTE_EMPLOYMENT_INFO # match the collection

input_document_collection_name: <DOC_COLLECTION_NAME>

golden_eval:
  source_sheet_uri: "<url provided by user>"
  accuracy_thresholds:
    unit: 1.0
    sample: 1.0

document_metadata_filter_query_template: |
  SELECT DISTINCT document_contents_id
  FROM `{input_document_collection_metadata_address}`
  WHERE document_contents_id IS NOT NULL
    AND DATE(document_update_datetime) >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY)

prompt_vars:
  state_agency_name: "<full agency name>"
  input_document_orientation: |
    <orientation text>
```

`document_update_datetime` is a standard column in every metadata table — use it
directly rather than adding a redundant date metadata column. Confirm with the
user if a different window is needed.

### 3. Reference data — two files required per state

Even if the state has no known organizations or acronyms yet, both files must
exist or the config test will fail:

- `recidiviz/documents/config/reference_data/{state_code_lower}/known_organizations.yaml`
- `recidiviz/documents/config/reference_data/{state_code_lower}/acronyms.yaml`

Minimal content for each:

```yaml
reference_data_type: known_organizations # or acronyms
entries: []
```

**Skip this step** if the state already has reference data files (i.e., other
CNI document collections already exist for this state).

### 4. `recidiviz/view_registry/deployed_views.py` — add 4 dataset IDs

**Skip this step** if the state already has
`us_{state}_document_extraction_results` entries in `deployed_views.py` (i.e.,
other CNI document collections already exist for this state).

Find the block of `us_{state}_document_extraction_results` entries (around
line 176) and insert the four dataset IDs for the new state in alphabetical
order among the others:

```python
"us_{state}_document_extraction_results",
"us_{state}_document_extraction_results__entity_mentions",
"us_{state}_document_extraction_results__pre_resolution",
"us_{state}_document_extraction_results__unvalidated",
```

### 5. Terraform BQ datasets — run the auto-generation script

**Skip this step** if step 4 was skipped.

`terraform_managed_bigquery_datasets.yaml` is auto-generated. Do **not** edit it
by hand. After the files above are in place, run:

```bash
uv run python -m recidiviz.tools.deploy.generate_terraform_managed_bigquery_datasets
```

This regenerates
`recidiviz/tools/deploy/terraform/config/terraform_managed_bigquery_datasets.yaml`
from the source-table repository. The CI test
`GenerateTerraformManagedBigqueryDatasetsTest` fails if this file is stale, so
the script must be run and the result committed.

### 6. Calculation docs — run the documentation generator

**Skip this step** if step 4 was skipped.

After adding dataset IDs to `deployed_views.py`, regenerate the calculation
documentation markdown files:

```bash
uv run python -m recidiviz.calculator.calculation_documentation_generator
```

This writes new
`docs/calculation/views/us_{state}_document_extraction_results*/` files. The CI
test `CalculationDocumentationGeneratorTest::test_generate_markdowns` fails if
these are missing, so the generator must be run and its output committed.

## Commit workflow

1. Stage all created/modified files.
2. Run the terraform generation script (step 5 above) and stage its output.
3. Run the calculation docs generator (step 6 above) and stage its output.
4. Commit. The pre-commit hook will run prettier on YAML files — if it reformats
   any file, re-stage the reformatted file and commit again.
5. If the `update_terraform_lock_file` hook fails with "Module not installed",
   run `terraform init -backend=false` in `recidiviz/tools/deploy/terraform/`
   (no GCS backend bucket required), then commit again.

## Verification

After committing, run the config-level tests to confirm everything wires up:

```bash
uv run pytest recidiviz/tests/documents/
```

If a `FileNotFoundError` for `known_organizations.yaml` or `acronyms.yaml`
appears, both reference data files must exist for every state that has
extractors, even when the entries list is empty.

## Follow-up tickets

After all steps are complete, ask the user:

> "Should I file follow-up Linear tickets for:
>
> 1. Filling out known organizations and acronyms for {state}
> 2. Filling out the golden eval sheet(s) and validating that accuracy targets
>    are reached for each collection
>
> If yes, I'll use the `create_recidiviz_data_issues` skill and add the
> **Project: Case Note Insights** label to each ticket."

If the user says yes, invoke the `create_recidiviz_data_issues` skill for each
ticket, adding "Project: Case Note Insights" and the state code label (e.g.
"US_TN") to each.
