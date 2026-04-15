# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Employment extraction pipeline with phase control and segmentation.

Replaces run_sandbox_employment_pipeline.sh with a Python script that supports
resumable, parallelizable extraction of up to 1M+ documents.

Phases:
  EMPLOYMENT_DOC_UPLOAD      Upload case note documents to GCS
  EMPLOYMENT_EXTRACTION      Extract employment info from case notes
  EMPLOYMENT_VIEW_DEPLOY     Deploy extraction views for employment extractor
  EMPLOYER_ER_DOC_UPLOAD     Upload employer context docs (derived from extraction)
  EMPLOYER_ER_EXTRACTION     Run employer entity resolution
  EMPLOYER_ER_VIEW_DEPLOY    Deploy extraction views for ER extractor
  SUMMARY_VIEW_DEPLOY        Deploy summary views + set table expiration

Usage:
    # Check status of all phases:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_employment_pipeline \
        --project_id recidiviz-staging \
        --state_code US_IX \
        --sandbox_dataset_prefix anna_employment \
        --sandbox_documents_bucket recidiviz-staging-anna-scratch

    # Run one phase (with optional segmentation):
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_employment_pipeline \
        --project_id recidiviz-staging \
        --state_code US_IX \
        --sandbox_dataset_prefix anna_employment \
        --sandbox_documents_bucket recidiviz-staging-anna-scratch \
        --phase EMPLOYMENT_EXTRACTION --segment_index 0 --total_segments 10
"""
import argparse
import datetime
import enum
import logging
import sys

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    get_extractor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_STORE_METADATA_DATASET_ID,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.views.view_config import (
    get_document_extraction_current_summary_view_builders,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job import (
    SANDBOX_DATASET_EXPIRATION_MS,
    _build_input_document_query,
    _build_not_for_prod_address_overrides,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_extraction_pipeline_phase import (
    PhaseStatus,
    _get_doc_upload_status,
    _get_extraction_status,
    run_doc_upload,
    run_extraction,
    run_view_deploy,
    set_table_expiration,
)
from recidiviz.tools.load_views_to_sandbox import load_collected_views_to_sandbox
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Extractor ID suffixes (state code is prepended at runtime)
_PRIMARY_EXTRACTOR_SUFFIX = "CASE_NOTE_EMPLOYMENT_INFO"
_ER_EXTRACTOR_SUFFIX = "CASE_NOTE_EMPLOYER_ENTITY_RESOLUTION"


def _primary_extractor_id(state_code: str) -> str:
    return f"{state_code}_{_PRIMARY_EXTRACTOR_SUFFIX}"


def _er_extractor_id(state_code: str) -> str:
    return f"{state_code}_{_ER_EXTRACTOR_SUFFIX}"


class EmploymentPhase(enum.Enum):
    EMPLOYMENT_DOC_UPLOAD = "EMPLOYMENT_DOC_UPLOAD"
    EMPLOYMENT_EXTRACTION = "EMPLOYMENT_EXTRACTION"
    EMPLOYMENT_VIEW_DEPLOY = "EMPLOYMENT_VIEW_DEPLOY"
    EMPLOYER_ER_DOC_UPLOAD = "EMPLOYER_ER_DOC_UPLOAD"
    EMPLOYER_ER_EXTRACTION = "EMPLOYER_ER_EXTRACTION"
    EMPLOYER_ER_VIEW_DEPLOY = "EMPLOYER_ER_VIEW_DEPLOY"
    SUMMARY_VIEW_DEPLOY = "SUMMARY_VIEW_DEPLOY"


def _get_phase_config(
    state_code: str,
) -> dict[EmploymentPhase, tuple[str, str]]:
    """Builds the phase-to-(extractor_id, phase_type) mapping for a state."""
    primary = _primary_extractor_id(state_code)
    er = _er_extractor_id(state_code)
    return {
        EmploymentPhase.EMPLOYMENT_DOC_UPLOAD: (primary, "DOC_UPLOAD"),
        EmploymentPhase.EMPLOYMENT_EXTRACTION: (primary, "EXTRACTION"),
        EmploymentPhase.EMPLOYMENT_VIEW_DEPLOY: (primary, "VIEW_DEPLOY"),
        EmploymentPhase.EMPLOYER_ER_DOC_UPLOAD: (er, "DOC_UPLOAD"),
        EmploymentPhase.EMPLOYER_ER_EXTRACTION: (er, "EXTRACTION"),
        EmploymentPhase.EMPLOYER_ER_VIEW_DEPLOY: (er, "VIEW_DEPLOY"),
        EmploymentPhase.SUMMARY_VIEW_DEPLOY: (primary, "SUMMARY_VIEW_DEPLOY"),
    }


def _count_expected_extraction_docs(
    bq_client: BigQueryClientImpl,
    extractor: LLMPromptExtractorMetadata,
    sandbox_dataset_prefix: str,
) -> int | None:
    """Counts documents in the sandbox metadata table for an extractor.

    Since extraction processes all docs in the metadata table (no additional
    filtering), this is just a count of the uploaded docs.

    Returns None if the doc metadata table doesn't exist (DOC_UPLOAD not run).
    """
    doc_store_metadata_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, DOCUMENT_STORE_METADATA_DATASET_ID
    )
    document_query = _build_input_document_query(
        extractor=extractor,
        doc_store_metadata_dataset=doc_store_metadata_dataset,
        max_documents_to_process=None,
        sample_entity_count=None,
        active_in_compartment=None,
        lookback_days=None,
    )
    count_query = f"SELECT COUNT(*) as cnt FROM ({document_query})"
    try:
        rows = bq_client.run_query_async(query_str=count_query, use_query_cache=True)
        for row in rows:
            return row["cnt"]
    except Exception:
        return None
    return None


def show_status(
    state_code: str,
    sandbox_dataset_prefix: str,
) -> None:
    """Prints completion status of all employment pipeline phases."""
    bq_client = BigQueryClientImpl()
    primary_extractor = get_extractor(_primary_extractor_id(state_code))
    er_extractor = get_extractor(_er_extractor_id(state_code))
    assert isinstance(primary_extractor, LLMPromptExtractorMetadata)
    assert isinstance(er_extractor, LLMPromptExtractorMetadata)

    upload_status, upload_count = _get_doc_upload_status(
        bq_client, primary_extractor, sandbox_dataset_prefix
    )
    expected_extraction_docs = _count_expected_extraction_docs(
        bq_client, primary_extractor, sandbox_dataset_prefix
    )
    extraction_status, extraction_count = _get_extraction_status(
        bq_client,
        primary_extractor,
        sandbox_dataset_prefix,
        expected_doc_count=expected_extraction_docs,
    )
    er_upload_status, er_upload_count = _get_doc_upload_status(
        bq_client, er_extractor, sandbox_dataset_prefix
    )
    expected_er_docs = _count_expected_extraction_docs(
        bq_client, er_extractor, sandbox_dataset_prefix
    )
    er_extraction_status, er_extraction_count = _get_extraction_status(
        bq_client,
        er_extractor,
        sandbox_dataset_prefix,
        expected_doc_count=expected_er_docs,
    )

    print("\nEmployment extraction pipeline:")

    def _fmt(status: PhaseStatus, count: int, total: int | None = None) -> str:
        if total is not None and status == PhaseStatus.PARTIAL:
            return f"{status.value:14s} ({count}/{total} docs)"
        if total is not None and status == PhaseStatus.COMPLETE:
            return f"{status.value:14s} ({count}/{total} docs)"
        return f"{status.value:14s} ({count} docs)"

    ready = f"{'READY':14s} (idempotent)"
    print(f"  EMPLOYMENT_DOC_UPLOAD      {_fmt(upload_status, upload_count)}")
    print(
        f"  EMPLOYMENT_EXTRACTION      "
        f"{_fmt(extraction_status, extraction_count, expected_extraction_docs)}"
    )
    print(f"  EMPLOYMENT_VIEW_DEPLOY     {ready}")
    print(f"  EMPLOYER_ER_DOC_UPLOAD     {_fmt(er_upload_status, er_upload_count)}")
    print(
        f"  EMPLOYER_ER_EXTRACTION     "
        f"{_fmt(er_extraction_status, er_extraction_count, expected_er_docs)}"
    )
    print(f"  EMPLOYER_ER_VIEW_DEPLOY    {ready}")
    print(f"  SUMMARY_VIEW_DEPLOY        {ready}")
    print()


def run_phase(  # pylint: disable=too-many-positional-arguments
    phase: EmploymentPhase,
    state_code: str,
    sandbox_dataset_prefix: str,
    sandbox_documents_bucket: str,
    sample_entity_count: int | None,
    lookback_days: int | None,
    active_in_compartment: str | None,
    segment_index: int | None,
    total_segments: int | None,
    max_llm_concurrency: int,
    max_llm_rps: float,
    force: bool = False,
) -> None:
    """Runs a single employment pipeline phase."""
    phase_config = _get_phase_config(state_code)
    extractor_id, phase_type = phase_config[phase]

    phase_start = datetime.datetime.now()
    logging.info("=" * 60)
    logging.info("  %s  (started %s)", phase.value, phase_start.strftime("%H:%M:%S"))
    logging.info("=" * 60)

    if phase_type == "DOC_UPLOAD":
        upload_failures = run_doc_upload(
            extractor_id=extractor_id,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            sandbox_documents_bucket=sandbox_documents_bucket,
            sample_entity_count=sample_entity_count,
            lookback_days=lookback_days,
            active_in_compartment=active_in_compartment,
            segment_index=segment_index,
            total_segments=total_segments,
        )
        if upload_failures > 0 and not force:
            raise SystemExit(1)
    elif phase_type == "EXTRACTION":
        extraction_result = run_extraction(
            extractor_id=extractor_id,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            sandbox_documents_bucket=sandbox_documents_bucket,
            segment_index=segment_index,
            total_segments=total_segments,
            max_llm_concurrency=max_llm_concurrency,
            max_llm_rps=max_llm_rps,
        )
        if (
            extraction_result is not None
            and extraction_result.num_failed_extractions > 0
            and not force
        ):
            raise SystemExit(1)
    elif phase_type == "VIEW_DEPLOY":
        run_view_deploy(
            extractor_id=extractor_id,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )
    elif phase_type == "SUMMARY_VIEW_DEPLOY":
        # Deploy summary views (sessions, current summary)
        all_builders = get_document_extraction_current_summary_view_builders()
        builders = [b for b in all_builders if "case_note_employment_info" in b.view_id]
        if builders:
            logging.info(
                "Deploying %d summary view builders to sandbox...", len(builders)
            )
            input_source_table_overrides = _build_not_for_prod_address_overrides(
                sandbox_dataset_prefix
            )
            load_collected_views_to_sandbox(
                sandbox_dataset_prefix=sandbox_dataset_prefix,
                state_code_filter=None,
                collected_builders=builders,
                input_source_table_dataset_overrides_dict=None,
                input_source_table_overrides=input_source_table_overrides,
                allow_slow_views=True,
                rematerialize_changed_views_only=False,
                failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
                schemas_only=False,
                default_table_expiration_ms=SANDBOX_DATASET_EXPIRATION_MS,
            )
        # Set table expiration
        set_table_expiration(sandbox_dataset_prefix)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses command-line arguments for the employment pipeline."""
    parser = argparse.ArgumentParser(
        description="Employment extraction pipeline with phase control."
    )
    parser.add_argument(
        "--project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--state_code",
        type=str,
        required=True,
        help="State code (e.g. US_IX, US_CO). Used to build extractor IDs.",
    )
    parser.add_argument("--sandbox_dataset_prefix", type=str, required=True)
    parser.add_argument("--sandbox_documents_bucket", type=str, required=True)
    parser.add_argument(
        "--phase",
        type=str,
        choices=[p.value for p in EmploymentPhase],
        default=None,
        help="Phase to run. Omit to show status of all phases.",
    )
    parser.add_argument("--segment_index", type=int, default=None)
    parser.add_argument("--total_segments", type=int, default=None)
    parser.add_argument("--sample_entity_count", type=int, default=None)
    parser.add_argument("--lookback_days", type=int, default=None)
    parser.add_argument("--active_in_compartment", type=str, default=None)
    parser.add_argument("--max_llm_concurrency", type=int, default=200)
    parser.add_argument(
        "--max_llm_rps",
        type=float,
        default=50.0,
        help=(
            "Max LLM requests per second. Caps burst rate to avoid Vertex AI "
            "per-second 429s. Default 50."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Continue even if there are extraction failures.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv[1:])

    # Validate segment args
    if (args.segment_index is None) != (args.total_segments is None):
        print("Error: --segment_index and --total_segments must be used together")
        sys.exit(1)
    if args.segment_index is not None and args.total_segments is not None:
        if args.segment_index < 0 or args.segment_index >= args.total_segments:
            print(f"Error: --segment_index must be in [0, {args.total_segments - 1}]")
            sys.exit(1)

    parsed_state_code = args.state_code.upper()

    with local_project_id_override(args.project_id):
        if args.phase is None:
            show_status(parsed_state_code, args.sandbox_dataset_prefix)
        else:
            parsed_phase = EmploymentPhase(args.phase)
            run_phase(
                phase=parsed_phase,
                state_code=parsed_state_code,
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                sandbox_documents_bucket=args.sandbox_documents_bucket,
                sample_entity_count=args.sample_entity_count,
                lookback_days=args.lookback_days,
                active_in_compartment=args.active_in_compartment,
                segment_index=args.segment_index,
                total_segments=args.total_segments,
                max_llm_concurrency=args.max_llm_concurrency,
                max_llm_rps=args.max_llm_rps,
                force=args.force,
            )
