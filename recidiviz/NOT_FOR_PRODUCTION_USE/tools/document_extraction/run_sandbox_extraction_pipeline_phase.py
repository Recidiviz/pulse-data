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
"""Runs a single phase of an extraction pipeline for one extractor.

Supports three phase types:
  - DOC_UPLOAD:  Refresh the document store for this extractor's input collection
  - EXTRACTION:  Run LLM extraction using ConcurrentExtractionProcessor
  - VIEW_DEPLOY: Deploy extraction views for this extractor

DOC_UPLOAD and EXTRACTION phases support segmentation
(--segment_index / --total_segments) to allow multiple workers to process
different document subsets in parallel. EXTRACTION phases detect completion
and no-op if all documents are already extracted.

Typically called by pipeline-specific scripts (e.g. run_sandbox_employment_pipeline.py)
but can also be invoked directly for any extractor.

Usage:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_extraction_pipeline_phase \
        --project_id recidiviz-staging \
        --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
        --sandbox_dataset_prefix anna_employment \
        --sandbox_documents_bucket recidiviz-staging-anna-scratch \
        --phase EXTRACTION --segment_index 0 --total_segments 4
"""
import argparse
import datetime
import enum
import logging
import sys

import pytz

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.concurrent_extraction_processor import (
    ConcurrentExtractionProcessor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    get_extractor,
    get_extractor_collection,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    GCSDocumentProvider,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_result_metadata import (
    ExtractionJobResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_STORE_METADATA_DATASET_ID,
    get_document_collection_config,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection import (
    main as refresh_document_collection_main,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job import (
    _build_input_document_query,
    _check_prerequisite_extractor,
    _create_sandbox_source_tables,
    _deploy_extraction_views_to_sandbox,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Default expiration applied to sandbox datasets/tables touched by any phase
# that adds data. Idempotent — safe to apply repeatedly.
EXPIRATION_DAYS = 90

# Sandbox datasets created by extraction pipelines. The expiration bumper
# walks this list and updates each dataset (default expiration) and each
# existing table within it.
_SANDBOX_DATASET_SUFFIXES = [
    "document_extraction_metadata",
    "document_extraction_results__raw",
    "document_extraction_results__validated",
    "document_extraction_results__exclusions",
    "document_extraction_results",
    "document_store_metadata",
    "llm_extraction_views",
]


def set_table_expiration(sandbox_dataset_prefix: str) -> None:
    """Bumps expiration on all tables in the sandbox datasets to 90 days.

    Idempotent — safe to call from any phase that adds or modifies data.
    Skips datasets that don't exist yet. Only existing tables are touched;
    future tables created within the pipeline will be bumped by the next
    phase's call to this function.
    """
    bq_client = BigQueryClientImpl()
    expiration_datetime = datetime.datetime.now(tz=pytz.UTC) + datetime.timedelta(
        days=EXPIRATION_DAYS
    )
    datasets = [
        f"{sandbox_dataset_prefix}_{suffix}" for suffix in _SANDBOX_DATASET_SUFFIXES
    ]

    for dataset_id in datasets:
        if not bq_client.dataset_exists(dataset_id):
            continue
        for table_item in bq_client.list_tables(dataset_id):
            bq_client.set_table_expiration(
                BigQueryAddress(dataset_id=dataset_id, table_id=table_item.table_id),
                expiration_datetime,
            )

    logging.info("Table expiration set to %d days.", EXPIRATION_DAYS)


class Phase(enum.Enum):
    DOC_UPLOAD = "DOC_UPLOAD"
    EXTRACTION = "EXTRACTION"
    VIEW_DEPLOY = "VIEW_DEPLOY"


class PhaseStatus(enum.Enum):
    NOT_STARTED = "NOT_STARTED"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"
    READY = "READY"


def _wrap_query_with_segment(
    query: str,
    segment_index: int,
    total_segments: int,
) -> str:
    """Wraps a document query with a FARM_FINGERPRINT segment filter.

    Uses ABS() because FARM_FINGERPRINT returns signed INT64 and BigQuery's MOD
    preserves the sign of the dividend — without ABS, negative hash values
    produce negative remainders that don't match any segment index.
    """
    return f"""
        SELECT * FROM ({query}) sub
        WHERE MOD(ABS(FARM_FINGERPRINT(document_id)), {total_segments}) = {segment_index}
    """


def _get_extraction_status(
    bq_client: BigQueryClientImpl,
    extractor: LLMPromptExtractorMetadata,
    sandbox_dataset_prefix: str,
    expected_doc_count: int | None = None,
) -> tuple[PhaseStatus, int]:
    """Returns (status, extracted_count) for the EXTRACTION phase."""
    raw_address = DocumentExtractionResultMetadata.raw_table_address(
        state_code=extractor.state_code,
        collection_name=extractor.collection_name,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )
    if not bq_client.table_exists(raw_address):
        return PhaseStatus.NOT_STARTED, 0

    full_address = raw_address.to_project_specific_address(bq_client.project_id)
    query = f"""
        SELECT COUNT(DISTINCT document_id) as cnt
        FROM `{full_address.to_str()}`
        WHERE extractor_version_id = '{extractor.extractor_version_id()}'
          AND status = 'SUCCESS'
    """
    rows = bq_client.run_query_async(query_str=query, use_query_cache=True)
    extracted_count = 0
    for row in rows:
        extracted_count = row["cnt"]

    if extracted_count == 0:
        return PhaseStatus.NOT_STARTED, 0
    if expected_doc_count is not None and extracted_count >= expected_doc_count:
        return PhaseStatus.COMPLETE, extracted_count
    return PhaseStatus.PARTIAL, extracted_count


def _get_doc_upload_status(
    bq_client: BigQueryClientImpl,
    extractor: LLMPromptExtractorMetadata,
    sandbox_dataset_prefix: str,
) -> tuple[PhaseStatus, int]:
    """Returns (status, doc_count) for the DOC_UPLOAD phase."""
    doc_collection_config = get_document_collection_config(
        extractor.input_document_collection_name
    )
    metadata_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, DOCUMENT_STORE_METADATA_DATASET_ID
    )
    metadata_address = doc_collection_config.metadata_table_address(
        dataset_override=metadata_dataset
    )

    if not bq_client.table_exists(metadata_address):
        return PhaseStatus.NOT_STARTED, 0

    full_address = metadata_address.to_project_specific_address(bq_client.project_id)
    query = f"SELECT COUNT(*) as cnt FROM `{full_address.to_str()}`"
    rows = bq_client.run_query_async(query_str=query, use_query_cache=True)
    doc_count = 0
    for row in rows:
        doc_count = row["cnt"]

    if doc_count == 0:
        return PhaseStatus.NOT_STARTED, 0
    return PhaseStatus.COMPLETE, doc_count


def run_doc_upload(
    extractor_id: str,
    sandbox_dataset_prefix: str,
    sandbox_documents_bucket: str,
    sample_entity_count: int | None,
    lookback_days: int | None,
    active_in_compartment: str | None,
    segment_index: int | None = None,
    total_segments: int | None = None,
    person_ids: list[int] | None = None,
) -> int:
    """Runs the DOC_UPLOAD phase for an extractor.

    Returns the number of failed uploads.
    """
    extractor = get_extractor(extractor_id)
    if not isinstance(extractor, LLMPromptExtractorMetadata):
        raise ValueError(
            f"Extractor {extractor_id} is not an LLMPromptExtractorMetadata"
        )

    doc_collection_name = extractor.input_document_collection_name
    logging.info("DOC_UPLOAD: refreshing collection %s", doc_collection_name)

    # For segmented doc upload, we pass sample_size=None and let the refresh
    # script handle the full set. Segmentation for doc upload is handled by
    # the refresh script's internal batching and the fact that GCS uploads
    # are idempotent (same doc_id → same GCS path).
    if segment_index is not None and total_segments is not None:
        logging.warning(
            "DOC_UPLOAD segmentation is not yet fully supported. "
            "Running full upload (idempotent, safe to run from multiple workers)."
        )

    doc_collection_config = get_document_collection_config(doc_collection_name)
    bq_client = BigQueryClientImpl()

    # Check prerequisite if this is a derived collection
    if doc_collection_config.prerequisite_extractor_id:
        _check_prerequisite_extractor(
            bq_client, doc_collection_config, sandbox_dataset_prefix
        )

    num_failures = refresh_document_collection_main(
        collection_name=doc_collection_name,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        sandbox_bucket=sandbox_documents_bucket,
        sample_size=None,
        sample_entity_count=sample_entity_count,
        active_in_compartment=active_in_compartment,
        lookback_days=lookback_days,
        person_ids=person_ids,
    )

    if num_failures > 0:
        logging.error(
            "DOC_UPLOAD had %d failures. Re-run this phase to retry failed "
            "uploads, or use --force to continue despite failures.",
            num_failures,
        )

    set_table_expiration(sandbox_dataset_prefix)
    return num_failures


def run_extraction(  # pylint: disable=too-many-positional-arguments
    extractor_id: str,
    sandbox_dataset_prefix: str,
    sandbox_documents_bucket: str,
    segment_index: int | None = None,
    total_segments: int | None = None,
    # Concurrency cap: max in-flight LLM requests.
    max_llm_concurrency: int = 200,
    # Rate cap: max requests per second to send to the LLM. Prevents Vertex
    # AI burst-limit 429s that occur when all N concurrent requests fire
    # simultaneously at job start.
    max_llm_rps: float = 50.0,
) -> ExtractionJobResultMetadata | None:
    """Runs the EXTRACTION phase for an extractor.

    Extracts all documents present in the sandbox metadata table (no additional
    filtering). Document selection should have been done during DOC_UPLOAD.

    Returns ExtractionJobResultMetadata, or None if all docs already extracted.
    """
    extractor = get_extractor(extractor_id)
    if not isinstance(extractor, LLMPromptExtractorMetadata):
        raise ValueError(
            f"Extractor {extractor_id} is not an LLMPromptExtractorMetadata"
        )

    collection = get_extractor_collection(extractor.collection_name)
    bq_client = BigQueryClientImpl()

    # Create sandbox source tables
    _create_sandbox_source_tables(bq_client, extractor, sandbox_dataset_prefix)

    # Check doc store metadata exists
    doc_store_metadata_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, DOCUMENT_STORE_METADATA_DATASET_ID
    )
    doc_collection_config = get_document_collection_config(
        extractor.input_document_collection_name
    )
    doc_metadata_address = doc_collection_config.metadata_table_address(
        dataset_override=doc_store_metadata_dataset
    )
    if not bq_client.table_exists(doc_metadata_address):
        logging.error(
            "Document store metadata table [%s] does not exist. "
            "Run DOC_UPLOAD phase first.",
            doc_metadata_address.to_str(),
        )
        return None

    # Build document query — select all docs in metadata table, no filtering.
    # Document selection (sampling, lookback, compartment) was done during
    # DOC_UPLOAD; we extract everything that was uploaded.
    document_query = _build_input_document_query(
        extractor=extractor,
        doc_store_metadata_dataset=doc_store_metadata_dataset,
        max_documents_to_process=None,
        sample_entity_count=None,
        active_in_compartment=None,
        lookback_days=None,
    )

    # Apply segmentation filter
    if segment_index is not None and total_segments is not None:
        document_query = _wrap_query_with_segment(
            document_query, segment_index, total_segments
        )
        logging.info(
            "EXTRACTION: segment %d/%d for %s",
            segment_index,
            total_segments,
            extractor.extractor_id,
        )
    else:
        logging.info("EXTRACTION: processing all docs for %s", extractor.extractor_id)

    # Get documents
    document_provider = GCSDocumentProvider(
        document_id_bq_query=document_query,
        state_code=extractor.state_code,
        sandbox_bucket=sandbox_documents_bucket,
    )
    documents = list(document_provider.document_iterator(bq_client))
    logging.info("Found %d documents to process", len(documents))

    if not documents:
        logging.info("No documents found. Nothing to do.")
        return None

    # Check completion before running
    processor = ConcurrentExtractionProcessor(
        extractor=extractor,
        collection=collection,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        max_llm_concurrency=max_llm_concurrency,
        max_llm_rps=max_llm_rps,
    )
    already_extracted = processor.get_already_extracted_document_ids(bq_client)
    remaining = [d for d in documents if d.document_id not in already_extracted]

    result: ExtractionJobResultMetadata | None = None
    if not remaining:
        segment_label = ""
        if segment_index is not None and total_segments is not None:
            segment_label = f" segment {segment_index}/{total_segments}"
        logging.info(
            "EXTRACTION%s: already complete (%d docs). Skipping extraction.",
            segment_label,
            len(documents),
        )
    else:
        logging.info(
            "%d/%d documents remaining (skipping %d already extracted)",
            len(remaining),
            len(documents),
            len(documents) - len(remaining),
        )
        result = processor.process_documents(bq_client, remaining)

    if result is not None and result.num_failed_extractions > 0:
        logging.error(
            "EXTRACTION had %d failures out of %d documents. "
            "Re-run this phase to retry failed documents, or use --force to "
            "continue despite failures.",
            result.num_failed_extractions,
            result.num_documents_originally_submitted,
        )

    set_table_expiration(sandbox_dataset_prefix)
    return result


def run_view_deploy(
    extractor_id: str,
    sandbox_dataset_prefix: str,
) -> None:
    """Runs the VIEW_DEPLOY phase for an extractor."""
    extractor = get_extractor(extractor_id)
    if not isinstance(extractor, LLMPromptExtractorMetadata):
        raise ValueError(
            f"Extractor {extractor_id} is not an LLMPromptExtractorMetadata"
        )

    logging.info("VIEW_DEPLOY: deploying views for %s", extractor.extractor_id)
    _deploy_extraction_views_to_sandbox(extractor, sandbox_dataset_prefix)
    set_table_expiration(sandbox_dataset_prefix)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses command-line arguments for the single-extractor phase tool."""
    parser = argparse.ArgumentParser(
        description="Run a single extractor with phase control and segmentation."
    )
    parser.add_argument(
        "--project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument("--extractor_id", type=str, required=True)
    parser.add_argument("--sandbox_dataset_prefix", type=str, required=True)
    parser.add_argument("--sandbox_documents_bucket", type=str, required=True)
    parser.add_argument(
        "--phase",
        type=str,
        choices=[p.value for p in Phase],
        required=True,
        help="Phase to run: DOC_UPLOAD, EXTRACTION, or VIEW_DEPLOY.",
    )
    parser.add_argument("--segment_index", type=int, default=None)
    parser.add_argument("--total_segments", type=int, default=None)
    parser.add_argument("--sample_entity_count", type=int, default=None)
    parser.add_argument("--lookback_days", type=int, default=None)
    parser.add_argument("--active_in_compartment", type=str, default=None)
    parser.add_argument(
        "--person_ids",
        type=str,
        default=None,
        help=(
            "Comma-separated list of person IDs to restrict documents to "
            "(e.g., 12345,67890). Cannot be used with --active_in_compartment "
            "or --sample_entity_count."
        ),
    )
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

    person_id_list: list[int] | None = None
    if args.person_ids:
        person_id_list = [int(p.strip()) for p in args.person_ids.split(",")]

    with local_project_id_override(args.project_id):
        if args.phase == Phase.DOC_UPLOAD.value:
            upload_failures = run_doc_upload(
                extractor_id=args.extractor_id,
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                sandbox_documents_bucket=args.sandbox_documents_bucket,
                sample_entity_count=args.sample_entity_count,
                lookback_days=args.lookback_days,
                active_in_compartment=args.active_in_compartment,
                segment_index=args.segment_index,
                total_segments=args.total_segments,
                person_ids=person_id_list,
            )
            if upload_failures > 0 and not args.force:
                sys.exit(1)
        elif args.phase == Phase.EXTRACTION.value:
            extraction_result = run_extraction(
                extractor_id=args.extractor_id,
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
                sandbox_documents_bucket=args.sandbox_documents_bucket,
                segment_index=args.segment_index,
                total_segments=args.total_segments,
                max_llm_concurrency=args.max_llm_concurrency,
                max_llm_rps=args.max_llm_rps,
            )
            if (
                extraction_result is not None
                and extraction_result.num_failed_extractions > 0
                and not args.force
            ):
                sys.exit(1)
        elif args.phase == Phase.VIEW_DEPLOY.value:
            run_view_deploy(
                extractor_id=args.extractor_id,
                sandbox_dataset_prefix=args.sandbox_dataset_prefix,
            )
