# Pipelines

All pipelines are Apache Beam pipelines deployed on Google Cloud Dataflow using
flex templates. Every pipeline extends `BasePipeline` in `base_pipeline.py`.

## Pipeline Types

- **`ingest/`** - Parent package for ingest pipelines:
  - **`activity/`** - Ingests raw state data into normalized Recidiviz entities. Reads
    configurations from `recidiviz/ingest/direct/regions/`.
  - **`identity/`** - Clusters person records for identity resolution.
- **`metrics/`** - Computes metrics from normalized entities. Sub-pipelines cover
  incarceration, supervision, recidivism, population spans, program, and violation.
- **`supplemental/`** - Produces supplemental datasets, typically state-specific
  (e.g., `us_ix_case_note_extracted_entities`, `us_me_snoozed_opportunities`).

## Key Files

- `base_pipeline.py` - Abstract base class all pipelines extend
- `pipeline_names.py` - Registry of all pipeline names
- `pipeline_parameters.py` - Base pipeline parameter definitions
- `flex_pipeline_runner.py` - Entry point for running a pipeline by name
- `calculation_pipeline_templates.yaml` - Flex template definitions
- `dataflow_config.py` - Dataflow-specific configuration
