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
"""Script to run document extraction jobs for all extractors in a collection.

Orchestrates running multiple state-specific extractors that share the same
extractor collection, then deploys collection-level union views that combine
results across all states.

Usage:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_collection_extraction_job \
        --project_id recidiviz-staging \
        --collection_name CASE_NOTE_HOUSING_INFO \
        --sandbox_dataset_prefix my_prefix \
        --sandbox_documents_bucket recidiviz-staging-my-scratch \
        concurrent

    # Only run specific states with entity sampling:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_collection_extraction_job \
        --project_id recidiviz-staging \
        --collection_name CASE_NOTE_HOUSING_INFO \
        --sandbox_dataset_prefix my_prefix \
        --sandbox_documents_bucket recidiviz-staging-my-scratch \
        --state_codes US_CO,US_IX \
        --sample_entity_count_per_state 5 \
        concurrent
"""
import argparse
import logging
import sys

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    collect_extractors,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.views.extraction_view_collector import (
    DOCUMENT_STORE_METADATA_DATASET,
    VALIDATED_DATASET,
    generate_context_metadata_union_sql,
    get_document_extraction_view_builders,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.views.document_view_schemas import (
    CONTEXT_METADATA_VIEW_SCHEMA,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.views.view_config import (
    get_document_extraction_current_summary_view_builders,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection import (
    main as refresh_document_collection_main,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job import (
    SANDBOX_DATASET_EXPIRATION_MS,
    _build_not_for_prod_address_overrides,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job import (
    main as run_extraction_main,
)
from recidiviz.tools.load_views_to_sandbox import load_collected_views_to_sandbox
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def _deploy_collection_views_to_sandbox(
    ran_extractors: list[LLMPromptExtractorMetadata],
    sandbox_dataset_prefix: str,
    include_summary_builders: bool = True,
) -> None:
    """Deploys collection-level views to sandbox datasets.

    Constructs a custom union view that only includes the states that were
    actually run (to avoid referencing non-existent sandbox tables), then
    deploys per-extractor views and the custom union.
    """
    collection_name = ran_extractors[0].collection_name
    collection_view_id = collection_name.lower()

    # Get all production builders
    all_extraction_builders = get_document_extraction_view_builders()
    # Collect per-extractor builders for only the states we ran. Use startswith
    # so that derived per-extractor views (e.g. *_entities) are included alongside
    # the base validated view.
    ran_extractor_ids = {e.extractor_id.lower() for e in ran_extractors}
    per_extractor_builders: list[BigQueryViewBuilder] = []
    ran_validated_parents: list[BigQueryViewBuilder] = []
    ran_entities_parents: list[SimpleBigQueryViewBuilder] = []
    collection_entities_view_id = f"{collection_view_id}_entities"

    for builder in all_extraction_builders:
        is_per_extractor = any(
            builder.view_id.startswith(extractor_id)
            for extractor_id in ran_extractor_ids
        )
        if is_per_extractor:
            per_extractor_builders.append(builder)
            if builder.dataset_id == VALIDATED_DATASET:
                if builder.view_id in ran_extractor_ids:
                    ran_validated_parents.append(builder)
                elif builder.view_id.endswith("_entities") and isinstance(
                    builder, SimpleBigQueryViewBuilder
                ):
                    ran_entities_parents.append(builder)
        # Skip the production collection-level union views — we'll build our own
        # with only the states that were actually run.
        elif builder.view_id in (collection_view_id, collection_entities_view_id):
            continue
        elif (
            builder.dataset_id == DOCUMENT_STORE_METADATA_DATASET
            and "_entity_resolution" in collection_view_id
            and builder.view_id
            == collection_view_id.replace("_entity_resolution", "_context_metadata")
        ):
            # Context metadata union view for this ER collection — build a
            # custom version that only includes the states that were actually
            # run (same principle as the custom extraction results union above).
            # Guard against non-ER collections (which have no context metadata
            # view) and against unrelated ER collections' metadata views.
            assert isinstance(builder, SimpleBigQueryViewBuilder)
            per_extractor_builders.append(
                SimpleBigQueryViewBuilder(
                    dataset_id=DOCUMENT_STORE_METADATA_DATASET,
                    view_id=builder.view_id,
                    description=builder.description,
                    view_query_template=generate_context_metadata_union_sql(
                        ran_extractors
                    ),
                    should_materialize=True,
                    clustering_fields=["person_id"],
                    schema=CONTEXT_METADATA_VIEW_SCHEMA,
                )
            )

    # Build a custom validated union with only ran states' validated builders as parents
    assert len(ran_validated_parents) > 0
    custom_union_builder = UnionAllBigQueryViewBuilder(
        dataset_id=VALIDATED_DATASET,
        view_id=collection_view_id,
        description=(
            f"Union of validated extraction results for {collection_name} "
            f"(sandbox: {len(ran_validated_parents)} states)."
        ),
        parents=ran_validated_parents,
        clustering_fields=["state_code", "person_id"],
        schema=ran_validated_parents[0].build().schema,
    )

    # Combine builders: per-extractor + custom union + (optionally) summaries
    # Filter summary builders to only those for the current collection
    all_builders: list[BigQueryViewBuilder] = [
        *per_extractor_builders,
        custom_union_builder,
    ]
    if include_summary_builders:
        all_summary_builders = get_document_extraction_current_summary_view_builders()
        all_builders += [
            b for b in all_summary_builders if collection_view_id in b.view_id
        ]

    # If the collection has entities views, also build a custom entities union.
    if ran_entities_parents:
        all_builders.append(
            UnionAllBigQueryViewBuilder(
                dataset_id=VALIDATED_DATASET,
                view_id=collection_entities_view_id,
                description=(
                    f"Union of resolved entities for {collection_name} "
                    f"(sandbox: {len(ran_entities_parents)} states)."
                ),
                parents=ran_entities_parents,
                clustering_fields=["state_code", "person_id"],
                schema=ran_entities_parents[0].schema,
            )
        )

    # Build overrides directly to skip validation against the standard source
    # table repo (these datasets live in NOT_FOR_PRODUCTION_USE).
    input_source_table_overrides = _build_not_for_prod_address_overrides(
        sandbox_dataset_prefix
    )

    logging.info(
        "Deploying collection-level views to sandbox (%d builders)...",
        len(all_builders),
    )
    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        state_code_filter=None,
        collected_builders=all_builders,
        input_source_table_dataset_overrides_dict=None,
        input_source_table_overrides=input_source_table_overrides,
        allow_slow_views=True,
        rematerialize_changed_views_only=False,
        failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
        schemas_only=False,
        default_table_expiration_ms=SANDBOX_DATASET_EXPIRATION_MS,
    )


def main(
    *,
    collection_name: str,
    sandbox_dataset_prefix: str,
    sandbox_documents_bucket: str,
    mode: str,
    sample_size: int | None = None,
    sample_entity_count_per_state: int | None = None,
    state_codes: list[StateCode] | None = None,
    sandbox_llm_job_artifact_bucket: str | None = None,
    active_in_compartment: str | None = None,
    lookback_days: int | None = None,
    person_ids: list[int] | None = None,
    label: tuple[str, str] | None = None,
) -> None:
    """Runs extraction jobs for all extractors in a collection."""
    # Discover extractors for this collection
    all_extractors = collect_extractors()
    collection_extractors = [
        e
        for e in all_extractors.values()
        if isinstance(e, LLMPromptExtractorMetadata)
        and e.collection_name == collection_name
    ]

    if not collection_extractors:
        logging.error("No extractors found for collection %s", collection_name)
        return

    # Filter by state_codes if provided
    if state_codes:
        state_code_set = set(state_codes)
        collection_extractors = [
            e for e in collection_extractors if e.state_code in state_code_set
        ]
        if not collection_extractors:
            logging.error(
                "No extractors found for collection %s with state codes %s",
                collection_name,
                [s.value for s in state_codes],
            )
            return

    logging.info(
        "Found %d extractors for collection %s:",
        len(collection_extractors),
        collection_name,
    )
    for ext in sorted(collection_extractors, key=lambda e: e.extractor_id):
        logging.info("  %s (state: %s)", ext.extractor_id, ext.state_code.value)

    # Step 1: Refresh document collections for each unique input collection
    seen_doc_collections: set[str] = set()
    for ext in collection_extractors:
        if ext.input_document_collection_name not in seen_doc_collections:
            seen_doc_collections.add(ext.input_document_collection_name)
            logging.info(
                "Refreshing document collection: %s",
                ext.input_document_collection_name,
            )
            refresh_document_collection_main(
                collection_name=ext.input_document_collection_name,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
                sandbox_bucket=sandbox_documents_bucket,
                sample_size=sample_size,
                sample_entity_count=sample_entity_count_per_state,
                active_in_compartment=active_in_compartment,
                lookback_days=lookback_days,
                person_ids=person_ids,
            )

    # Step 2: Run extraction for each extractor (without view deployment)
    for ext in sorted(collection_extractors, key=lambda e: e.extractor_id):
        logging.info("Running extraction for %s...", ext.extractor_id)
        run_extraction_main(
            extractor_id=ext.extractor_id,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            sandbox_documents_bucket=sandbox_documents_bucket,
            sample_size=sample_size,
            sample_entity_count=sample_entity_count_per_state,
            mode=mode,
            sandbox_llm_job_artifact_bucket=sandbox_llm_job_artifact_bucket,
            active_in_compartment=active_in_compartment,
            lookback_days=lookback_days,
            person_ids=person_ids,
            deploy_views=False,
            label=label,
        )

    # Step 3: Deploy all views at the end with a custom union
    _deploy_collection_views_to_sandbox(collection_extractors, sandbox_dataset_prefix)

    logging.info("=" * 60)
    logging.info("Collection extraction complete for %s!", collection_name)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser(
        description="Run document extraction jobs for all extractors in a collection."
    )

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
        help="The GCP project ID to use.",
    )
    parser.add_argument(
        "--collection_name",
        dest="collection_name",
        type=str,
        required=True,
        help="The extractor collection name (e.g., CASE_NOTE_HOUSING_INFO).",
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        type=str,
        required=True,
        help="Prefix for sandbox BQ datasets.",
    )
    parser.add_argument(
        "--sandbox_documents_bucket",
        dest="sandbox_documents_bucket",
        type=str,
        required=True,
        help="The GCS bucket containing documents.",
    )
    parser.add_argument(
        "--state_codes",
        dest="state_codes",
        type=str,
        default=None,
        help="Comma-separated list of state codes to run (e.g., US_CO,US_IX). If not specified, runs all states.",
    )
    parser.add_argument(
        "--sample_size",
        dest="sample_size",
        type=int,
        default=None,
        help="Maximum number of documents to process per extractor.",
    )
    parser.add_argument(
        "--sample_entity_count_per_state",
        dest="sample_entity_count_per_state",
        type=int,
        default=None,
        help="Sample N entities per state and process all their documents.",
    )
    parser.add_argument(
        "--active_in_compartment",
        dest="active_in_compartment",
        type=str,
        default=None,
        help="Restrict to people with an active session in the given compartment.",
    )
    parser.add_argument(
        "--lookback_days",
        dest="lookback_days",
        type=int,
        default=None,
        help="Only include documents within the last N days.",
    )
    parser.add_argument(
        "--person_ids",
        dest="person_ids",
        type=str,
        default=None,
        help=(
            "Comma-separated list of person IDs to restrict documents to "
            "(e.g., 12345,67890). Cannot be used with --active_in_compartment, "
            "--sample_size, or --sample_entity_count_per_state."
        ),
    )
    parser.add_argument(
        "--label",
        dest="label",
        type=str,
        default=None,
        metavar="KEY=VALUE",
        help=(
            "Optional Vertex AI label in KEY=VALUE format added to each LLM "
            "request so per-task costs are visible in the GCP billing export "
            "(e.g., --label variant=flash-lite-thinking-off)."
        ),
    )

    # Subparsers for modes
    subparsers = parser.add_subparsers(
        title="LLM client modes",
        description="Choose which LLM client to use",
        dest="mode",
    )

    subparsers.add_parser("fake", help="Use FakeLLMClient for testing")
    subparsers.add_parser(
        "concurrent",
        help="Local concurrent processing via LiteLLM (uses Vertex AI ADC)",
    )

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

    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args = parse_arguments(sys.argv[1:])

    if not known_args.mode:
        print("Error: mode is required (fake, concurrent, or batch)")
        sys.exit(1)

    state_code_list: list[StateCode] | None = None
    if known_args.state_codes:
        state_code_list = [
            StateCode(s.strip()) for s in known_args.state_codes.split(",")
        ]

    person_id_list: list[int] | None = None
    if known_args.person_ids:
        if (
            known_args.active_in_compartment
            or known_args.sample_entity_count_per_state
            or known_args.sample_size
        ):
            print(
                "Error: --person_ids cannot be used with --active_in_compartment, "
                "--sample_entity_count_per_state, or --sample_size"
            )
            sys.exit(1)
        person_id_list = [int(pid.strip()) for pid in known_args.person_ids.split(",")]

    parsed_label: tuple[str, str] | None = None
    if known_args.label:
        if "=" not in known_args.label:
            print("Error: --label must be in KEY=VALUE format")
            sys.exit(1)
        key, _, value = known_args.label.partition("=")
        parsed_label = (key, value)

    with local_project_id_override(known_args.project_id):
        main(
            collection_name=known_args.collection_name,
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            sandbox_documents_bucket=known_args.sandbox_documents_bucket,
            mode=known_args.mode,
            sample_size=known_args.sample_size,
            sample_entity_count_per_state=known_args.sample_entity_count_per_state,
            state_codes=state_code_list,
            sandbox_llm_job_artifact_bucket=getattr(
                known_args, "sandbox_llm_job_artifact_bucket", None
            ),
            active_in_compartment=known_args.active_in_compartment,
            lookback_days=known_args.lookback_days,
            person_ids=person_id_list,
            label=parsed_label,
        )
