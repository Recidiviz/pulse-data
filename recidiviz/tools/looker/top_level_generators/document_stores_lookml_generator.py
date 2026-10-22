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
"""Generates LookML views and explores for document store review.

For every configured (state, document_collection) pair (excluding US_OZ) this
generator produces:
  - A metadata view over {state}_document_store_metadata.{collection}
  - A document-contents view over {state}_document_contents.{collection}_document_contents
  - For each extractor whose input_document_collection_name matches:
    - A doc-level extraction-results view over
      {state}_document_extraction_results.{extractor_collection}_materialized
    - One array-level fanout view per ARRAY_OF_STRUCT field in the extractor schema
  - A single explore joining all of the above on document_contents_id

The generated files are written to:
  views/document_stores/{state}/
  explores/document_stores/

Each generated explore uses extension: required (template pattern). After adding a
new (state, collection), add a concrete explore entry to the Looker model files
(recidiviz-staging.model.lkml and recidiviz-123.model.lkml):

  explore: us_xx_collection_documents {
    extends: [us_xx_collection_documents_template]
  }

Example usage:
    python -m recidiviz.tools.looker.top_level_generators.document_stores_lookml_generator
"""
import os
import shutil
from collections import defaultdict

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    load_first_order_llm_extractor_configs,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    load_first_order_document_collection_configs,
)
from recidiviz.tools.looker.constants import (
    EXPLORES_DIR,
    GENERATED_LOOKML_ROOT_PATH,
    VIEWS_DIR,
)
from recidiviz.tools.looker.document_stores.document_store_explore_generator import (
    DOCUMENT_STORES_EXPLORES_SUBDIR,
    write_explore,
)
from recidiviz.tools.looker.document_stores.document_store_view_generator import (
    DOCUMENT_STORES_VIEWS_SUBDIR,
    write_array_field_view,
    write_document_contents_view,
    write_extraction_results_view,
    write_metadata_view,
)
from recidiviz.tools.looker.top_level_generators.base_lookml_generator import (
    LookMLGenerator,
)

# US_OZ is the playground/demo state — excluded from production Looker output.
_EXCLUDED_STATES = frozenset({StateCode.US_OZ})

_SOURCE_SCRIPT_PATH = __file__


def _clear_output_dirs(output_dir: str) -> None:
    """Removes all previously generated document-store LKml files."""
    views_dir = os.path.join(output_dir, VIEWS_DIR, DOCUMENT_STORES_VIEWS_SUBDIR)
    explores_dir = os.path.join(
        output_dir, EXPLORES_DIR, DOCUMENT_STORES_EXPLORES_SUBDIR
    )
    for path in (views_dir, explores_dir):
        shutil.rmtree(path, ignore_errors=True)


def _extractor_configs_by_input_collection(
    state_configs: dict[str, LLMExtractorConfig],
) -> dict[str, list[LLMExtractorConfig]]:
    """Returns extractor configs grouped by their input document collection name."""
    by_collection: dict[str, list[LLMExtractorConfig]] = defaultdict(list)
    for config in state_configs.values():
        by_collection[config.input_document_collection.name].append(config)
    return by_collection


def _generate_for_state(
    *,
    output_dir: str,
    doc_collection_configs: dict[str, DocumentCollectionConfig],
    extractor_configs_by_collection: dict[str, list[LLMExtractorConfig]],
) -> None:
    """Generates all views and one explore per document collection."""
    views_dir = os.path.join(output_dir, VIEWS_DIR)
    explores_dir = os.path.join(output_dir, EXPLORES_DIR)

    for collection_config in doc_collection_configs.values():
        write_metadata_view(
            output_dir=views_dir,
            collection_config=collection_config,
            source_script_path=_SOURCE_SCRIPT_PATH,
        )
        write_document_contents_view(
            output_dir=views_dir,
            collection_config=collection_config,
            source_script_path=_SOURCE_SCRIPT_PATH,
        )

        extractor_configs = extractor_configs_by_collection.get(
            collection_config.name, []
        )
        for extractor_config in extractor_configs:
            write_extraction_results_view(
                output_dir=views_dir,
                extractor_config=extractor_config,
                source_script_path=_SOURCE_SCRIPT_PATH,
            )
            array_fields = (
                extractor_config.extractor_collection.output_schema.array_of_struct_user_fields
            )
            for array_field in array_fields:
                write_array_field_view(
                    output_dir=views_dir,
                    extractor_config=extractor_config,
                    array_field=array_field,
                    source_script_path=_SOURCE_SCRIPT_PATH,
                )

        write_explore(
            output_dir=explores_dir,
            collection_config=collection_config,
            extractor_configs=extractor_configs,
            source_script_path=_SOURCE_SCRIPT_PATH,
        )


def generate_document_stores_lookml(output_dir: str) -> None:
    """Generates all document-store LKml files and writes them to |output_dir|."""
    _clear_output_dirs(output_dir)

    extractor_configs_by_state = load_first_order_llm_extractor_configs()

    # Drive iteration off document collections so states that have collections
    # but no extractors still get their metadata and contents views generated.
    for state_code in StateCode:
        if state_code in _EXCLUDED_STATES:
            continue
        doc_collection_configs = load_first_order_document_collection_configs(
            state_code
        )
        if not doc_collection_configs:
            continue
        _generate_for_state(
            output_dir=output_dir,
            doc_collection_configs=doc_collection_configs,
            extractor_configs_by_collection=_extractor_configs_by_input_collection(
                extractor_configs_by_state.get(state_code, {})
            ),
        )


class DocumentStoresLookMLGenerator(LookMLGenerator):
    """Generates LookML views and explores for document store review."""

    @staticmethod
    def generate_lookml(output_dir: str) -> None:
        generate_document_stores_lookml(output_dir)


if __name__ == "__main__":
    DocumentStoresLookMLGenerator.generate_lookml(output_dir=GENERATED_LOOKML_ROOT_PATH)
