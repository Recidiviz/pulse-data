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
"""Synthesizes the resolved entity-resolution `LLMExtractorConfig` binding an ER
extractor collection to one state.

An ER extractor mirrors a first-order `extractor.yaml`-backed extractor, but every
part is generated rather than authored: it reads the composite-document collection,
selects every composite document, inherits the parent state extractor's resolved
reference data (whose org aliases drive the clustering) and its retry setting,
applies the framework's entity-resolution-specific document-count caps (lower than
the first-order caps because ER's thinking calls are slower and its composite
documents are pricier per document), and runs the ER collection's framework-fixed
thinking-enabled model. The standard runtime thread runs the resulting config
unchanged.
"""
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_ER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
    DEFAULT_ER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    LLMExtractorCollectionConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
    LLMExtractorDocumentFilterConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import LLMModelRegistry
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.utils.string_formatting import fix_indent


class EntityResolutionExtractorGenerator:
    """Synthesizes the resolved ER `LLMExtractorConfig` binding an ER extractor
    collection to one state. See the module docstring.
    """

    @staticmethod
    def _build_document_filter_query_template(
        entity_resolution_document_collection: DocumentCollectionConfig,
    ) -> str:
        """Returns the document metadata filter query template selecting every
        composite document in the ER document collection — the ER analogue of a
        first-order extractor's authored filter.
        """
        query = f"""
        SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
        FROM `{entity_resolution_document_collection.metadata_table_address.format_address_for_query_template()}`
        WHERE {DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NOT NULL
        """
        return fix_indent(query, indent_level=0)

    def generate(
        self,
        *,
        entity_resolution_collection: LLMExtractorCollectionConfig,
        first_order_config: LLMExtractorConfig,
        entity_group: EntityGroupConfig,
        entity_resolution_document_collection: DocumentCollectionConfig,
        model_registry: LLMModelRegistry,
    ) -> LLMExtractorConfig:
        """Returns the fully-resolved ER extractor config for one (ER collection,
        state), reading the composite-document collection, inheriting the parent
        first-order extractor's resolved reference data and retry setting, applying
        the framework's ER-specific document-count caps, and binding the ER
        collection's default (thinking-enabled) model resolved from
        |model_registry|.
        """
        model_config = model_registry.get_model_config(
            entity_resolution_collection.default_model_config_name
        )
        return LLMExtractorConfig(
            state_code=first_order_config.state_code,
            input_document_collection=entity_resolution_document_collection,
            document_filter=LLMExtractorDocumentFilterConfig(
                document_metadata_filter_query_template=self._build_document_filter_query_template(
                    entity_resolution_document_collection
                ),
                is_sandbox_config=False,
                document_limit=None,
                root_entity_ids=None,
            ),
            # The ER prompt template has no extractor-specific template variables - it's
            # fully state-agnostic.
            prompt_vars={},
            max_transient_retry_count=first_order_config.max_transient_retry_count,
            total_pending_document_count_hard_cap=DEFAULT_ER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
            single_job_document_count_batch_threshold=DEFAULT_ER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
            extractor_collection=entity_resolution_collection,
            model_config=model_config,
            reference_data=first_order_config.reference_data,
            entity_group=entity_group,
        )
