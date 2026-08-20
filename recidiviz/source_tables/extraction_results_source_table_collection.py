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
"""Contains source table definitions for the BigQuery tables the LLM
document-extraction pipeline writes: the three per-state, per-collection tables
(raw results, validated results, and the validation audit) and the golden eval
results table.
"""

from types import ModuleType

from recidiviz.common.constants.states import StateCode
from recidiviz.documents import config as default_config_module
from recidiviz.documents.dataset_config import (
    document_extraction_golden_eval_results_dataset,
    document_extraction_raw_results_dataset_for_region,
    document_extraction_validated_results_dataset_for_region,
    document_extraction_validation_audit_dataset_for_region,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionRawResultsBQTable,
    ExtractionValidatedResultsBQTable,
    ExtractionValidationAuditBQTable,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    load_llm_extractor_collection_configs,
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
            collection_name = config.extractor_collection.name
            raw_collection.add_source_table(
                table_id=ExtractionRawResultsBQTable.table_id(collection_name),
                description=ExtractionRawResultsBQTable.description(
                    collection_name=collection_name, state_code=state_code
                ),
                schema_fields=ExtractionRawResultsBQTable.schema(),
            )
            validated_collection.add_source_table(
                table_id=ExtractionValidatedResultsBQTable.table_id(collection_name),
                description=ExtractionValidatedResultsBQTable.description(
                    collection_name=collection_name, state_code=state_code
                ),
                schema_fields=ExtractionValidatedResultsBQTable.schema(),
            )
            audit_collection.add_source_table(
                table_id=ExtractionValidationAuditBQTable.table_id(collection_name),
                description=ExtractionValidationAuditBQTable.description(
                    collection_name=collection_name, state_code=state_code
                ),
                schema_fields=ExtractionValidationAuditBQTable.schema(),
            )

        collections.append(raw_collection)
        collections.append(validated_collection)
        collections.append(audit_collection)

    return collections


def collect_extraction_results_source_table_collections_for_config(
    config: LLMExtractorConfig,
) -> list[SourceTableCollection]:
    """Collects the extraction source table collections for a single
    extractor config"""
    return collect_extraction_results_source_table_collections(
        configs={config.state_code: {config.extractor_collection.name: config}}
    )


def collect_golden_eval_results_source_table_collection(
    *, config_module: ModuleType | None = None
) -> SourceTableCollection:
    """Collects the golden eval results source table collection — one
    `document_extraction_golden_eval_results.{collection_name}` table per extractor
    collection defined in |config_module| (the production config package by
    default) — so one definition creates the production tables and, via
    `.as_sandbox_collection(prefix)`, the sandbox-prefixed ones.

    The tables are state-agnostic: every state that runs a given extractor writes
    to the same table, keyed apart by the `state_code` column.
    """
    collection = SourceTableCollection(
        dataset_id=document_extraction_golden_eval_results_dataset(),
        labels=[],
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description=(
            "Golden eval result tables for LLM document extraction, one per "
            "extractor collection. Append-only."
        ),
    )

    module = config_module or default_config_module
    for collection_config in load_llm_extractor_collection_configs(module).values():
        collection_name = collection_config.name
        collection.add_source_table(
            table_id=GoldenEvalResultsBQTable.table_id(collection_name),
            description=GoldenEvalResultsBQTable.description(
                collection_name=collection_name
            ),
            schema_fields=GoldenEvalResultsBQTable.schema(),
        )

    return collection
