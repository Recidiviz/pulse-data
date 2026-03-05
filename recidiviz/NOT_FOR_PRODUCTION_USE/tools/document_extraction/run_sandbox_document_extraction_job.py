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
"""Script to run a document extraction job in a sandbox using LiteLLM.

Usage:
    # Fake mode (for testing without real LLM calls)
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
        --project_id recidiviz-staging \
        --extractor_id US_IX_EMPLOYMENT \
        --sandbox_dataset_prefix ageiduschek \
        --sandbox_documents_bucket recidiviz-staging-anna-scratch \
        fake

    # Concurrent mode (local processing via Vertex AI)
    # Requires: gcloud auth application-default login
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
        --project_id recidiviz-staging \
        --extractor_id US_IX_EMPLOYMENT \
        --sandbox_dataset_prefix ageiduschek \
        --sandbox_documents_bucket recidiviz-staging-anna-scratch \
        concurrent

    # Batch mode (server-side processing via Vertex AI)
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
        --project_id recidiviz-staging \
        --extractor_id US_IX_EMPLOYMENT \
        --sandbox_dataset_prefix ageiduschek \
        --sandbox_documents_bucket recidiviz-staging-anna-scratch \
        batch \
        --sandbox_llm_job_artifact_bucket recidiviz-staging-anna-scratch
"""
import argparse
import logging
import os
import sys
import time

import recidiviz.NOT_FOR_PRODUCTION_USE.source_tables
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extraction_job import (
    LLMPromptExtractionJobResultProcessor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    collect_extractors,
    get_extractor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    GCSDocumentProvider,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.fake_llm_client import (
    FakeLLMClient,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.litellm_batch_client import (
    LiteLLMBatchClient,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.litellm_client import (
    LiteLLMClient,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_provider_delegate import (
    get_llm_provider_delegate,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.dataset_config import (
    EXTRACTION_METADATA_DATASET_ID,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.views.extraction_view_collector import (
    UNVALIDATED_DATASET,
    VALIDATED_DATASET,
    get_document_extraction_view_builders,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.active_compartment_filter import (
    build_active_entities_cte_sql,
    build_active_entities_exists_filter,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_ID_COLUMN_NAME,
    DOCUMENT_STORE_METADATA_DATASET_ID,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
    get_document_collection_config,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.source_tables.document_extraction_raw_source_table_collector import (
    collect_document_extraction_raw_source_table_collection,
)
from recidiviz.source_tables.collect_source_tables_from_yamls import (
    collect_source_tables_from_yamls_by_dataset,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.tools.load_views_to_sandbox import load_collected_views_to_sandbox
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id


def _create_sandbox_source_tables(
    bq_client: BigQueryClient,
    extractor: LLMPromptExtractorMetadata,
    sandbox_dataset_prefix: str,
) -> None:
    """Creates sandbox source tables for extraction metadata and raw results.

    Uses SourceTableCollection.as_sandbox_collection() to create sandbox versions
    of the extraction metadata tables and the raw results table for the given
    extractor.
    """
    sandbox_collections: list[SourceTableCollection] = []

    # 1. Extraction metadata tables (from NOT_FOR_PRODUCTION_USE YAML-managed
    #    source tables, which are not included in the standard YAML repo)
    not_for_prod_yamls_root = os.path.join(
        os.path.dirname(recidiviz.NOT_FOR_PRODUCTION_USE.source_tables.__file__),
        "yaml_managed",
    )
    source_tables_by_dataset = collect_source_tables_from_yamls_by_dataset(
        yamls_root_path=not_for_prod_yamls_root
    )
    for dataset_id, source_tables in source_tables_by_dataset.items():
        if dataset_id != EXTRACTION_METADATA_DATASET_ID:
            continue
        collection = SourceTableCollection(
            dataset_id=dataset_id,
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            description=f"Source tables for {dataset_id}",
            labels=[],
        )
        for st in source_tables:
            collection.add_source_table(
                table_id=st.address.table_id,
                schema_fields=st.schema_fields,
                description=st.description,
            )
        sandbox_collections.append(
            collection.as_sandbox_collection(sandbox_dataset_prefix)
        )

    # 2. Raw results table for this extractor
    raw_collection = collect_document_extraction_raw_source_table_collection()
    raw_table_id = DocumentExtractionResultMetadata.raw_table_id(
        extractor.state_code, extractor.collection_name
    )
    # Build a filtered collection with only this extractor's table
    filtered_collection = SourceTableCollection(
        dataset_id=raw_collection.dataset_id,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description=raw_collection.description,
        labels=[],
    )
    for source_table in raw_collection.source_tables:
        if source_table.address.table_id == raw_table_id:
            filtered_collection.add_source_table(
                table_id=source_table.address.table_id,
                schema_fields=source_table.schema_fields,
                description=source_table.description,
            )
    sandbox_collections.append(
        filtered_collection.as_sandbox_collection(sandbox_dataset_prefix)
    )

    update_manager = SourceTableUpdateManager(bq_client)
    update_manager.update_async(
        source_table_collections=sandbox_collections,
        log_file=os.path.join(
            os.path.dirname(__file__),
            "logs/create_sandbox_source_tables.log",
        ),
        log_output=False,
    )

    # BigQuery streaming inserts require a brief delay after table creation
    logging.info("Waiting for newly created tables to be ready for streaming...")
    time.sleep(10)


def _build_not_for_prod_address_overrides(
    sandbox_dataset_prefix: str,
) -> BigQueryAddressOverrides:
    """Builds source table address overrides for NOT_FOR_PRODUCTION_USE datasets.

    These datasets are not in the standard source table repo, so we build
    overrides directly to avoid validation in address_overrides_for_input_source_tables.
    """
    raw_dataset = DocumentExtractionResultMetadata.RAW_DATASET_ID
    builder = BigQueryAddressOverrides.Builder(sandbox_prefix=None)
    for dataset_id in [
        raw_dataset,
        DOCUMENT_STORE_METADATA_DATASET_ID,
        VALIDATED_DATASET,
        UNVALIDATED_DATASET,
    ]:
        builder.register_custom_dataset_override(
            dataset_id,
            BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_dataset_prefix, dataset_id
            ),
            force_allow_custom=True,
        )
    return builder.build()


def _deploy_extraction_views_to_sandbox(
    extractor: LLMPromptExtractorMetadata,
    sandbox_dataset_prefix: str,
) -> None:
    """Deploys extraction result views to sandbox datasets.

    Filters view builders to those matching the current extractor and deploys
    them with source table dataset overrides pointing to the sandbox raw results
    and document store metadata.
    """
    all_builders: list[BigQueryViewBuilder] = [
        *get_document_extraction_view_builders(),
    ]
    extractor_view_id = extractor.extractor_id.lower()
    collection_view_id = extractor.collection_name.lower()
    # Include per-extractor views, collection-level union view, and derived
    # collection-level views (e.g., sessions, current_summary)
    extractor_builders = [
        b
        for b in all_builders
        if b.view_id.startswith(extractor_view_id)
        or b.view_id.startswith(collection_view_id)
    ]

    if not extractor_builders:
        logging.warning(
            "No extraction view builders found for extractor %s",
            extractor.extractor_id,
        )
        return

    # Build overrides directly to skip validation against the standard source
    # table repo (these datasets live in NOT_FOR_PRODUCTION_USE).
    input_source_table_overrides = _build_not_for_prod_address_overrides(
        sandbox_dataset_prefix
    )

    logging.info("Deploying extraction result views to sandbox...")
    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        state_code_filter=extractor.state_code,
        collected_builders=extractor_builders,
        input_source_table_dataset_overrides_dict=None,
        input_source_table_overrides=input_source_table_overrides,
        allow_slow_views=True,
        rematerialize_changed_views_only=False,
        failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
        schemas_only=False,
    )


def _build_input_document_query(
    extractor: LLMPromptExtractorMetadata,
    doc_store_metadata_dataset: str,
    max_documents_to_process: int | None,
    sample_entity_count: int | None,
    active_in_compartment: str | None = None,
    lookback_days: int | None = None,
) -> str:
    """Builds the query to get document IDs to process.

    If sample_entity_count is set, samples N entities using FARM_FINGERPRINT
    for deterministic randomization, then returns all documents for those entities.
    If max_documents_to_process is also set, it limits the final document count.
    If active_in_compartment is set, restricts to entities with an active session
    in the given compartment.
    If lookback_days is set, only includes documents whose document_update_datetime
    is within the last N days.
    """
    doc_collection_config = get_document_collection_config(
        extractor.input_document_collection_name
    )
    doc_metadata_address = doc_collection_config.metadata_table_address(
        dataset_override=doc_store_metadata_dataset
    )

    limit_clause = (
        f"LIMIT {max_documents_to_process}" if max_documents_to_process else ""
    )

    # Build active compartment CTE if requested
    active_entities_cte = ""
    if active_in_compartment:
        active_entities_cte = (
            build_active_entities_cte_sql(
                root_entity_type=doc_collection_config.root_entity_type,
                project_id=project_id(),
                state_code=extractor.state_code,
                active_in_compartment=active_in_compartment,
            )
            + ","
        )

    # Build entity-based sampling if requested
    if sample_entity_count:
        # Get entity columns based on root entity type
        entity_columns = [
            schema.name
            for schema in doc_collection_config.root_entity_type.column_schemas()
        ]
        entity_columns_str = ", ".join(entity_columns)

        # Build a composite key for deterministic sampling
        entity_key_expr = (
            "CONCAT("
            + ", '|', ".join(f"CAST({col} AS STRING)" for col in entity_columns)
            + ")"
        )

        # When active_in_compartment is set, filter sampled entities
        active_filter_in_sample = ""
        if active_in_compartment:
            active_filter_in_sample = "WHERE " + build_active_entities_exists_filter(
                root_entity_type=doc_collection_config.root_entity_type,
                table_alias="src",
            )

        lookback_where = (
            f"WHERE m.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}"
            f" >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)"
            if lookback_days is not None
            else ""
        )
        query_template = f"""
            WITH {active_entities_cte}
            sampled_entities AS (
                SELECT DISTINCT {entity_columns_str}
                FROM `{{project_id}}.{doc_metadata_address.dataset_id}.{doc_metadata_address.table_id}` src
                {active_filter_in_sample}
                ORDER BY FARM_FINGERPRINT({entity_key_expr})
                LIMIT {sample_entity_count}
            )
            SELECT DISTINCT m.{DOCUMENT_ID_COLUMN_NAME}
            FROM `{{project_id}}.{doc_metadata_address.dataset_id}.{doc_metadata_address.table_id}` m
            INNER JOIN sampled_entities s
                ON {" AND ".join(f"m.{col} = s.{col}" for col in entity_columns)}
            {lookback_where}
            {limit_clause}
        """
    elif active_in_compartment:
        # No entity sampling, but still filter by active compartment
        active_filter = build_active_entities_exists_filter(
            root_entity_type=doc_collection_config.root_entity_type,
            table_alias="m",
        )
        lookback_and = (
            f"AND m.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}"
            f" >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)"
            if lookback_days is not None
            else ""
        )
        query_template = f"""
            WITH {active_entities_cte}
            src AS (
                SELECT DISTINCT {DOCUMENT_ID_COLUMN_NAME}
                FROM `{{project_id}}.{doc_metadata_address.dataset_id}.{doc_metadata_address.table_id}` m
                WHERE {active_filter}
                {lookback_and}
            )
            SELECT {DOCUMENT_ID_COLUMN_NAME} FROM src
            {limit_clause}
        """
    else:
        lookback_where = (
            f"WHERE {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}"
            f" >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)"
            if lookback_days is not None
            else ""
        )
        query_template = f"""
            SELECT DISTINCT {DOCUMENT_ID_COLUMN_NAME}
            FROM `{{project_id}}.{doc_metadata_address.dataset_id}.{doc_metadata_address.table_id}`
            {lookback_where}
            {limit_clause}
        """

    query_builder = BigQueryQueryBuilder(
        parent_address_overrides=None,
        parent_address_formatter_provider=None,
    )
    return query_builder.build_query(
        project_id=project_id(),
        query_template=query_template,
        query_format_kwargs={},
    )


def main(
    *,
    extractor_id: str,
    sandbox_dataset_prefix: str,
    sandbox_documents_bucket: str,
    sample_size: int | None,
    sample_entity_count: int | None,
    mode: str,
    sandbox_llm_job_artifact_bucket: str | None = None,
    active_in_compartment: str | None = None,
    lookback_days: int | None = None,
    deploy_views: bool = True,
) -> None:
    """Runs a document extraction job using sandbox datasets."""

    # Get the extractor
    extractor = get_extractor(extractor_id)
    if not isinstance(extractor, LLMPromptExtractorMetadata):
        raise ValueError(
            f"Extractor {extractor_id} is not an LLMPromptExtractorMetadata. "
            f"Only LLM extractors are supported."
        )

    logging.info(
        "Extractor: %s (state: %s)", extractor.extractor_id, extractor.state_code.value
    )

    doc_store_metadata_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, DOCUMENT_STORE_METADATA_DATASET_ID
    )

    logging.info("Using sandbox dataset prefix: %s", sandbox_dataset_prefix)
    logging.info("  Documents GCS bucket: %s", sandbox_documents_bucket)
    if sandbox_llm_job_artifact_bucket:
        logging.info(
            "  LLM job artifact GCS bucket: %s", sandbox_llm_job_artifact_bucket
        )

    # Initialize clients
    bq_client = BigQueryClientImpl()
    llm_client: FakeLLMClient | LiteLLMClient | LiteLLMBatchClient
    if mode == "fake":
        logging.info("Using FakeLLMClient for testing")
        llm_client = FakeLLMClient()
    elif mode == "concurrent":
        logging.info("Using LiteLLMClient for local concurrent processing")
        llm_client = LiteLLMClient(
            batches_gcs_bucket=sandbox_documents_bucket,
        )
    elif mode == "batch":
        assert sandbox_llm_job_artifact_bucket is not None
        logging.info("Using LiteLLMBatchClient for server-side async processing")
        provider_delegate = get_llm_provider_delegate(
            extractor.llm_provider,
            gcs_bucket=sandbox_llm_job_artifact_bucket,
            project=project_id(),
        )
        llm_client = LiteLLMBatchClient(provider_delegate=provider_delegate)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    # Create sandbox source tables (extraction metadata + raw results)
    _create_sandbox_source_tables(bq_client, extractor, sandbox_dataset_prefix)

    # Check the metadata table for the input document collection exists
    doc_collection_config = get_document_collection_config(
        extractor.input_document_collection_name
    )
    doc_metadata_address = doc_collection_config.metadata_table_address(
        dataset_override=doc_store_metadata_dataset
    )
    if not bq_client.table_exists(doc_metadata_address):
        logging.error(
            "Document store metadata table [%s] does not exist - must have first run "
            "refresh_sandbox_document_collection script.",
            doc_metadata_address.to_str(),
        )
        return

    # Build the document query and create provider
    document_query = _build_input_document_query(
        extractor=extractor,
        doc_store_metadata_dataset=doc_store_metadata_dataset,
        max_documents_to_process=sample_size,
        sample_entity_count=sample_entity_count,
        active_in_compartment=active_in_compartment,
        lookback_days=lookback_days,
    )
    document_provider = GCSDocumentProvider(
        document_id_bq_query=document_query,
        state_code=extractor.state_code,
        sandbox_bucket=sandbox_documents_bucket,
    )

    # Submit the extraction job
    job = llm_client.submit_extraction_job(
        bq_client=bq_client,
        extractor=extractor,
        document_provider=document_provider,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    # Check if any documents were submitted
    if job is None:
        logging.info(
            "No new documents to extract (all already submitted or none found)."
        )
    else:
        logging.info("Created job: %s", job.job_id)

        # Process results with polling loop
        result_processor = LLMPromptExtractionJobResultProcessor(
            llm_client,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            extractor=extractor,
        )
        while True:
            job_result = result_processor.process_results(job, bq_client)
            if job_result is not None:
                break
            logging.info("Batch still processing, waiting 5 seconds...")
            time.sleep(5)

        # Report results
        logging.info("=" * 60)
        logging.info("Extraction job complete!")
        logging.info("  Job ID: %s", job.job_id)
        logging.info(
            "  Documents submitted: %d", job_result.num_documents_originally_submitted
        )
        logging.info(
            "  Successful extractions: %d", job_result.num_successful_extractions
        )
        logging.info("  Failed extractions: %d", job_result.num_failed_extractions)

    # Deploy views unless the caller is handling view deployment separately
    # (e.g., the collection-level sandbox script deploys a custom union).
    if deploy_views:
        _deploy_extraction_views_to_sandbox(extractor, sandbox_dataset_prefix)


def parse_arguments(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser(
        description="Run a document extraction job in a sandbox."
    )

    # Common arguments
    parser.add_argument(
        "--list_extractors",
        action="store_true",
        help="List all available extractors and exit.",
    )
    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=False,
        help="The GCP project ID to use.",
    )
    parser.add_argument(
        "--extractor_id",
        dest="extractor_id",
        type=str,
        required=False,
        help="The ID of the extractor to use (e.g., US_IX_EMPLOYMENT).",
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        type=str,
        required=False,
        help="Prefix for sandbox BQ datasets.",
    )
    parser.add_argument(
        "--sandbox_documents_bucket",
        dest="sandbox_documents_bucket",
        type=str,
        required=False,
        help="The GCS bucket containing documents.",
    )
    parser.add_argument(
        "--sample_size",
        dest="sample_size",
        type=int,
        default=None,
        help="Maximum number of documents to process. If not specified, processes all.",
    )
    parser.add_argument(
        "--sample_entity_count",
        dest="sample_entity_count",
        type=int,
        default=None,
        help="Sample N entities and process all their documents. Uses FARM_FINGERPRINT for deterministic sampling.",
    )
    parser.add_argument(
        "--active_in_compartment",
        dest="active_in_compartment",
        type=str,
        default=None,
        help=(
            "If provided, restricts entity sampling to people with an active "
            "session in the given compartment (e.g., SUPERVISION, INCARCERATION)."
        ),
    )
    parser.add_argument(
        "--lookback_days",
        dest="lookback_days",
        type=int,
        default=None,
        help=(
            "If provided, only includes documents whose document_update_datetime "
            "is within the last N days."
        ),
    )

    # Subparsers for modes
    subparsers = parser.add_subparsers(
        title="LLM client modes",
        description="Choose which LLM client to use",
        dest="mode",
    )

    # Fake mode - no extra args
    subparsers.add_parser("fake", help="Use FakeLLMClient for testing")

    # Concurrent mode - uses Vertex AI Application Default Credentials
    subparsers.add_parser(
        "concurrent",
        help="Local concurrent processing via LiteLLM (uses Vertex AI ADC)",
    )

    # Batch mode
    parser_batch = subparsers.add_parser(
        "batch", help="Server-side batch processing via Vertex AI"
    )
    parser_batch.add_argument(
        "--sandbox_llm_job_artifact_bucket",
        dest="sandbox_llm_job_artifact_bucket",
        type=str,
        required=True,
        help="The GCS bucket for LLM batch job artifacts.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv[1:])

    if known_args.list_extractors:
        extractors = collect_extractors()
        print("Available extractors:")
        for ext_id, ext in sorted(extractors.items()):
            print(f"  {ext_id}")
            print(f"    State: {ext.state_code.value}")
            print(f"    Collection: {ext.collection_name}")
            print(f"    Input docs: {ext.input_document_collection_name}")
        sys.exit(0)

    # Validate required arguments
    if not known_args.project_id:
        print("Error: --project_id is required")
        sys.exit(1)
    if not known_args.extractor_id:
        print("Error: --extractor_id is required")
        sys.exit(1)
    if not known_args.sandbox_dataset_prefix:
        print("Error: --sandbox_dataset_prefix is required")
        sys.exit(1)
    if not known_args.sandbox_documents_bucket:
        print("Error: --sandbox_documents_bucket is required")
        sys.exit(1)
    if not known_args.mode:
        print("Error: mode is required (fake, concurrent, or batch)")
        sys.exit(1)

    with local_project_id_override(known_args.project_id):
        main(
            extractor_id=known_args.extractor_id,
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            sandbox_documents_bucket=known_args.sandbox_documents_bucket,
            sample_size=known_args.sample_size,
            sample_entity_count=known_args.sample_entity_count,
            mode=known_args.mode,
            sandbox_llm_job_artifact_bucket=getattr(
                known_args, "sandbox_llm_job_artifact_bucket", None
            ),
            active_in_compartment=known_args.active_in_compartment,
            lookback_days=known_args.lookback_days,
        )
