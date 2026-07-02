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
"""Contains source table definitions for the three BigQuery tables the LLM
document-extraction pipeline writes per extractor collection: raw results,
validated results, and the validation audit.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_raw_results_dataset_for_region,
    document_extraction_validated_results_dataset_for_region,
    document_extraction_validation_audit_dataset_for_region,
)
from recidiviz.documents.extraction.extraction_results_columns import (
    build_raw_results_schema,
    build_validated_results_schema,
    build_validation_audit_schema,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.source_tables.source_table_config import (
    ExtractionResultsSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)


def collect_extraction_results_source_table_collections(
    *, configs: dict[StateCode, dict[str, LLMExtractorConfig]]
) -> list[SourceTableCollection]:
    """Collects source table definitions for the raw results, validated results,
    and validation audit tables of every extractor in |configs| (the resolved
    extractor configs keyed by state code and then by collection name).

    Produces three collections per state — one for each of the result datasets —
    each holding one table per collection in that state, named by the lowercased
    collection name.
    """
    collections: list[SourceTableCollection] = []

    for state_code, configs_by_collection_name in configs.items():
        if not configs_by_collection_name:
            continue

        labels = [
            StateSpecificSourceTableLabel(state_code=state_code),
            ExtractionResultsSourceTableLabel(state_code=state_code),
        ]

        # Raw results are the immutable source of truth that validated results and
        # all parsed views derive from, and that we re-validate against when rules
        # change. Validated results and the audit are append-only. None of the
        # three can be reconstructed without re-calling the model, so we protect
        # against field deletions and recreation.
        raw_collection = SourceTableCollection(
            dataset_id=document_extraction_raw_results_dataset_for_region(state_code),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            description=(
                f"Raw LLM document-extraction result tables for "
                f"{StateCode.get_state(state_code)}"
            ),
        )
        validated_collection = SourceTableCollection(
            dataset_id=document_extraction_validated_results_dataset_for_region(
                state_code
            ),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            description=(
                f"Validated LLM document-extraction result tables for "
                f"{StateCode.get_state(state_code)}"
            ),
        )
        audit_collection = SourceTableCollection(
            dataset_id=document_extraction_validation_audit_dataset_for_region(
                state_code
            ),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            description=(
                f"LLM document-extraction validation audit tables for "
                f"{StateCode.get_state(state_code)}"
            ),
        )

        for config in configs_by_collection_name.values():
            result_table_id = config.extractor_collection.name.lower()
            raw_collection.add_source_table(
                table_id=result_table_id,
                description=(
                    f"Raw model output for the [{config.extractor_collection.name}] "
                    f"collection in {StateCode.get_state(state_code)}. One row per "
                    "successful model call, holding the raw JSON before any "
                    "validation or quality filtering."
                ),
                schema_fields=build_raw_results_schema(),
            )
            validated_collection.add_source_table(
                table_id=result_table_id,
                description=(
                    f"Validated model output for the "
                    f"[{config.extractor_collection.name}] collection in "
                    f"{StateCode.get_state(state_code)}. One row per (document, "
                    "extractor version) that passed all extraction-error checks, "
                    "with quality-filter corrections applied."
                ),
                schema_fields=build_validated_results_schema(),
            )
            audit_collection.add_source_table(
                table_id=result_table_id,
                description=(
                    f"Validation audit for the [{config.extractor_collection.name}] "
                    f"collection in {StateCode.get_state(state_code)}. One row per "
                    "document per validation run recording the outcome and any "
                    "issues found."
                ),
                schema_fields=build_validation_audit_schema(),
            )

        collections.append(raw_collection)
        collections.append(validated_collection)
        collections.append(audit_collection)

    return collections
