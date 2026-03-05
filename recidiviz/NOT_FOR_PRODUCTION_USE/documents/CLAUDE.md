# Document Extraction & Storage

**NOT FOR PRODUCTION USE.** This package is experimental tooling for sandboxing
LLM-based extraction of structured information from case notes. It is not
integrated into the production DAG and should not be deployed as part of the
production pipeline. If you are not @mayukas or @ageiduschek, you probably
should not be using this package — reach out to one of them first.

This package provides two subsystems:

1. **Store** (`store/`) — Uploads documents from BigQuery to GCS and tracks
   metadata
2. **Extraction** (`extraction/`) — Extracts structured data from documents
   using LLMs

## Architecture Overview

```
                        STORE                                    EXTRACTION
 ┌──────────────────────────────────┐     ┌──────────────────────────────────────────────┐
 │ BQ source query                  │     │ Extractor YAML configs                       │
 │     ↓                            │     │     ↓                                        │
 │ NewDocumentIdentifier            │     │ LLMPromptExtractorMetadata                   │
 │   (find new docs, generate IDs)  │     │     ↓                                        │
 │     ↓                            │     │ LLMExtractionRequest (build prompt+doc)       │
 │ DocumentUploader                 │     │     ↓                                        │
 │   (upload text to GCS)           │     │ LLMClient.submit_extraction_job()             │
 │     ↓                            │     │   ├─ FakeLLMClient (testing)                  │
 │ DocumentStoreUpdater             │     │   ├─ LiteLLMClient (concurrent async)         │
 │   (record metadata to BQ)        │     │   └─ LiteLLMBatchClient (server-side batch)   │
 └──────────────────────────────────┘     │     ↓                                        │
                                          │ ExtractionJobResultProcessor.process_results() │
                                          │   (write per-doc + job results to BQ)         │
                                          └──────────────────────────────────────────────┘
```

The store runs first to populate GCS with documents. Then extraction reads those
documents from GCS and sends them to an LLM.

## Directory Structure

```
recidiviz/NOT_FOR_PRODUCTION_USE/documents/
├── store/
│   ├── config/document_collections/     # YAML: one per state+collection
│   │   ├── us_ix/case_notes.yaml
│   │   └── us_nd/contact_notes.yaml
│   ├── document_collection_config.py    # Loads YAML, defines schema
│   ├── document_store_utils.py          # GCS path helpers
│   ├── new_document_identifier.py       # Finds docs not yet uploaded
│   ├── document_uploader.py             # Uploads docs to GCS
│   └── document_store_updater.py        # Orchestrates full update
├── extraction/
│   ├── config/collections/llm_prompt/   # Grouped by collection
│   │   ├── case_note_employment_info/
│   │   │   ├── collection.yaml          # Output schema, description, threshold
│   │   │   ├── prompt_template.txt      # Base prompt template
│   │   │   ├── us_co_extractor.yaml     # State-specific extractors
│   │   │   ├── us_ix_extractor.yaml
│   │   │   └── ...
│   │   └── case_note_housing_info/
│   │       ├── collection.yaml
│   │       ├── prompt_template.txt
│   │       └── ...
│   ├── persisted_models/                # BQ-backed metadata data classes
│   ├── document_extractor_configs.py    # Loads extractor YAML configs
│   ├── extraction_output_schema.py      # Defines extraction field schemas
│   ├── document_provider.py             # Reads documents from GCS
│   ├── llm_client.py                    # Abstract LLM interface + data classes
│   ├── llm_provider_delegate.py         # Provider-specific behavior
│   ├── litellm_client.py               # Concurrent async LLM client
│   ├── litellm_batch_client.py          # Server-side batch API client
│   ├── fake_llm_client.py              # Deterministic fake for testing
│   └── document_extraction_job.py       # Result processing + BQ persistence
```

## YAML Configuration

### Document Collections (`store/config/document_collections/`)

Define what documents exist and how to find them. Path pattern:
`{state_code}/{collection_name}.yaml`

```yaml
collection_description: Case notes from supervision officers
root_entity_type: PERSON_EXTERNAL_ID    # or PERSON_ID, STAFF_ID, STAFF_EXTERNAL_ID
primary_key_columns:
  - name: note_id
    type: STRING
additional_metadata_columns:
  - name: note_type
    type: STRING
document_generation_query: |
  SELECT ... FROM `{project_id}.us_xx_raw_data...`
```

The `collection_name` is derived from the path: `US_IX_CASE_NOTES` (uppercased
state + filename). The `document_generation_query` uses `{project_id}` as a
template variable.

### Extractor Collections (`extraction/config/collections/llm_prompt/{name}/collection.yaml`)

Define *what* to extract — the shared output schema across states. Each
collection lives in its own directory alongside its prompt template and
state-specific extractors:

```yaml
name: CASE_NOTE_EMPLOYMENT_INFO
description: Extract employment information from case notes
confidence_threshold: 0.8  # optional, defaults to DEFAULT_LLM_CONFIDENCE_THRESHOLD (0.8)
output_schema:
  full_batch_description: An array of employment info extracted from case notes
  result_level_description: Employment info for one case note
  inferred_fields:
    - name: employer_name
      type: STRING
      required: false
      description: Name of the employer
    - name: employment_status
      type: ENUM
      required: true
      values: [EMPLOYED, UNEMPLOYED, UNKNOWN]
```

The `confidence_threshold` controls the minimum confidence score for a row to
appear in the validated output view. If any flat field in a row falls below this
threshold, the entire row is excluded.

Supported field types: `STRING`, `BOOLEAN`, `ENUM`, `INTEGER`, `FLOAT`,
`ARRAY_OF_STRUCT`.

`ARRAY_OF_STRUCT` allows extracting repeated structured data (e.g., multiple
employers or payment obligations) from a single document. Each element is a
struct whose sub-fields are individually wrapped in the standard extraction
envelope:

```yaml
inferred_fields:
  - name: employers
    type: ARRAY_OF_STRUCT
    description: List of employers mentioned in this note
    fields:
      - name: employer_name
        type: STRING
        required: true
      - name: employment_status
        type: ENUM
        values: [employed, unemployed, seeking]
        required: true
      - name: start_date
        type: STRING
```

In the **unvalidated** BQ view, each `ARRAY_OF_STRUCT` field becomes a native
BQ `ARRAY<STRUCT<...>>` where each sub-field is a STRUCT with `{value,
confidence_score, null_reason, citations_json}`. In the **validated** view,
sub-fields are flattened to typed values with `__null_reason` companions (no
top-level confidence filter is applied to `ARRAY_OF_STRUCT` fields since
confidence is per-sub-field).

Every extraction result automatically includes an `is_relevant` boolean field
and, per flat field: `{value, null_reason, confidence_score, citations}`.

### Extractors (`extraction/config/collections/llm_prompt/{collection}/{state}_extractor.yaml`)

Define *how* to extract — state-specific LLM configuration. Each extractor YAML
lives alongside its collection's `collection.yaml` and `prompt_template.txt`:

```yaml
collection_name: CASE_NOTE_EMPLOYMENT_INFO
llm_provider: vertex_ai     # also: openai, azure, bedrock, hosted_vllm
model: gemini-2.5-flash     # must not contain "/"
input_document_collection_name: US_IX_CASE_NOTES
prompt_vars:                 # Substituted into prompt_template.txt
  state_agency_name: "Idaho Department of Correction"
  state_specific_context: "..."
```

The `extractor_id` is derived as `{STATE_CODE}_{COLLECTION_NAME}` (e.g.,
`US_IX_CASE_NOTE_EMPLOYMENT_INFO`). The `{output_format_instructions}`
placeholder is rendered at config load time from the collection's output schema,
producing a fully-rendered `instructions_prompt` that is sent directly to the
LLM as the system message.

## Key Concepts

### Document IDs

Document IDs are SHA256 hashes generated from `state_code | document_text`. This
means the same document text in the same state always gets the same ID. The
hashing logic lives in `new_document_identifier.py:document_id_sql_clause()`.

### Persisted Models (`extraction/persisted_models/`)

All metadata classes use `@attr.define` and implement `as_metadata_row()` /
`from_metadata_row()` for BigQuery serialization. They map to tables in the
`document_extraction_metadata` dataset:

| Class | BQ Table |
|-------|----------|
| `DocumentExtractorCollectionMetadata` | `extractor_collections` |
| `LLMPromptExtractorMetadata` | `llm_prompt_extractors` |
| `ExtractionJobMetadata` | `extraction_jobs` |
| `ExtractionJobSubmittedDocumentMetadata` | `extraction_job_submitted_documents` |
| `DocumentExtractionResultMetadata` | `document_extraction_results` |
| `ExtractionJobResultMetadata` | `extraction_job_results` |

BQ table schemas are defined in
`recidiviz/source_tables/yaml_managed/document_extraction_metadata/`.

### LLM Client Hierarchy

`LLMClient` (abstract) defines `submit_batch()` and `submit_extraction_job()`.
`LLMResultReader` (abstract) defines `get_results()`. Concrete implementations:

- **`FakeLLMClient`** — Generates deterministic synthetic results. First
  document always fails (to test error handling). Used for local testing.
- **`LiteLLMClient`** — Processes requests concurrently using `acompletion()`.
  Stores progress to GCS so partial results survive restarts.
- **`LiteLLMBatchClient`** — Submits a JSONL file to the provider's batch API.
  Uses `LLMProviderDelegate` for provider-specific request/response handling.

### Extraction Views

Extraction results are exposed through two layers of auto-generated BQ views
per extractor, plus a collection-level union:

1. **Unvalidated** (`document_extraction_results__unvalidated`): All successful
   extractions with per-field STRUCT columns containing `{value,
   confidence_score, null_reason, citations_json}`. Deduped to the latest
   extraction per document.
2. **Validated** (`document_extraction_results`): Filtered to high-confidence,
   relevant extractions. Flat typed columns with `__null_reason` and
   `__citation` companions per field. Rows where any flat field's confidence
   falls below the collection's `confidence_threshold` are excluded.
3. **Union** (`document_extraction_results`): A `UnionAllBigQueryViewBuilder`
   combining validated results across all states in a collection.

View SQL is generated programmatically by `extraction_view_sql_generator.py`
based on the collection's `ExtractionOutputSchema`.

### Extraction Flow

1. `LLMClient.submit_extraction_job()` writes metadata to BQ, gets documents
   from the provider, calls `submit_batch()`, records submitted documents
2. `ExtractionJobResultProcessor.process_results()` calls
   `LLMResultReader.get_results()`, writes per-document results and job-level
   summary to BQ
3. If `get_results()` returns `LLMExtractionInProgressBatchStatus`,
   `process_results()` returns `None` — callers should poll

### Error Types

- **Per-document** (`DocumentExtractionErrorType`): `DOCUMENT_NOT_FOUND`,
  `LLM_MALFORMED_RESPONSE`, `SCHEMA_VALIDATION_FAILURE`, etc.
- **Per-job** (`ExtractionJobErrorType`): `JOB_TIMEOUT`,
  `JOB_RESULT_RETRIEVAL_FAILURE`, etc.

## Sandbox Scripts

Three scripts in `recidiviz/NOT_FOR_PRODUCTION_USE/tools/document_extraction/`
support local development. See `example_commands.sh` for copy-paste examples.

### 1. Refresh Document Collection

Uploads documents to a sandbox GCS bucket and records metadata:

```bash
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection \
    --project_id recidiviz-staging \
    --collection_name US_IX_CASE_NOTES \
    --sandbox_dataset_prefix my_prefix \
    --sandbox_bucket recidiviz-staging-my-scratch \
    --sample_size 100
```

Use `--list_collections` to see available collections.

### 2. Run Single Extractor

Runs extraction for one extractor against documents already in a sandbox. Must
run `refresh_sandbox_document_collection` first:

```bash
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
    --project_id recidiviz-staging \
    --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
    --sandbox_dataset_prefix my_prefix \
    --sandbox_documents_bucket recidiviz-staging-my-scratch \
    --sample_entity_count 10 \
    --active_in_compartment SUPERVISION \
    --lookback_days 90 \
    concurrent
```

Key options:
- `--sample_size N` — Limit to N documents total
- `--sample_entity_count N` — Sample N people and process all their documents
- `--active_in_compartment COMPARTMENT` — Restrict to people with an active
  session in the given compartment (e.g., `SUPERVISION`, `INCARCERATION`)
- `--lookback_days N` — Only include documents from the last N days
- Modes: `fake` (no LLM calls), `concurrent` (local async), `batch`
  (server-side)

Use `--list_extractors` to see available extractors.

### 3. Run Full Collection

Runs all extractors in a collection (handles document refresh, extraction for
each state, and union view deployment):

```bash
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_collection_extraction_job \
    --project_id recidiviz-staging \
    --collection_name CASE_NOTE_EMPLOYMENT_INFO \
    --sandbox_dataset_prefix my_prefix \
    --sandbox_documents_bucket recidiviz-staging-my-scratch \
    --state_codes US_IX \
    --sample_entity_count_per_state 5 \
    concurrent
```

Use `--state_codes US_IX,US_CO` to restrict to specific states.

## Common Development Tasks

### Adding a new document collection

1. Create `store/config/document_collections/{state}/{name}.yaml`
2. Write a `document_generation_query` that returns `document_text`,
   `document_update_datetime`, and columns matching your `root_entity_type` and
   `primary_key_columns`
3. Add a source table YAML in
   `recidiviz/source_tables/yaml_managed/document_store_metadata/` if the
   metadata table needs to be managed

### Adding a new extraction target

1. Create a collection directory at
   `extraction/config/collections/llm_prompt/{name}/` with:
   - `collection.yaml` defining the output schema
   - `prompt_template.txt` with the base prompt (use `{output_format_instructions}`
     placeholder and `{state_agency_name}`, `{state_specific_context}`, etc.
     for state-specific variables)
2. Create state-specific extractor YAMLs as `{state}_extractor.yaml` in the
   same directory, with `prompt_vars` to customize the base template
3. Set `input_document_collection_name` to match an existing document collection

### Adding extraction support for another state

Create a new `{state}_extractor.yaml` in the existing collection directory at
`extraction/config/collections/llm_prompt/{collection_name}/`. Each state can
use different models, providers, and prompts for the same collection.
