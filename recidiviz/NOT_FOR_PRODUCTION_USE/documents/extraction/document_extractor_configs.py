# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Functions for collecting and accessing extractor configurations from YAML files."""
import os
from functools import cache

from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_collection_metadata import (
    DocumentExtractorCollectionMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_metadata import (
    DocumentExtractorMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)

# Path to the config directory relative to this file
_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")
_COLLECTIONS_DIR = os.path.join(_CONFIG_DIR, "collections")


@cache
def collect_extractor_collections() -> dict[str, DocumentExtractorCollectionMetadata]:
    """Returns a map of extractor collection name to its configuration.

    Discovers collection.yaml files under collections/<type>/<collection_name>/.
    """
    collections: dict[str, DocumentExtractorCollectionMetadata] = {}

    for type_entry in os.scandir(_COLLECTIONS_DIR):
        if not type_entry.is_dir():
            continue

        for entry in os.scandir(type_entry.path):
            if not entry.is_dir():
                continue
            yaml_path = os.path.join(entry.path, "collection.yaml")
            collection = DocumentExtractorCollectionMetadata.from_yaml(yaml_path)

            if collection.name in collections:
                raise ValueError(
                    f"Duplicate extractor collection name '{collection.name}' "
                    f"found in {yaml_path}"
                )

            collections[collection.name] = collection

    return collections


def get_extractor_collection(
    collection_name: str,
) -> DocumentExtractorCollectionMetadata:
    """Returns the DocumentExtractorCollectionMetadata for the given name.

    Raises KeyError if the collection is not found.
    """
    collections = collect_extractor_collections()
    if collection_name not in collections:
        raise KeyError(
            f"DocumentExtractorCollectionMetadata '{collection_name}' not found. "
            f"Available collections: {list(collections.keys())}"
        )
    return collections[collection_name]


@cache
def collect_extractors() -> dict[str, DocumentExtractorMetadata]:
    """Returns a map of extractor ID to its configuration.

    Discovers *_extractor.yaml files under collections/<type>/<collection_name>/.
    DocumentExtractorMetadata IDs are unique identifiers like US_IX_CASE_NOTE_EMPLOYMENT_INFO.
    """
    extractors: dict[str, DocumentExtractorMetadata] = {}

    for type_entry in os.scandir(_COLLECTIONS_DIR):
        if not type_entry.is_dir():
            continue

        for entry in os.scandir(type_entry.path):
            if not entry.is_dir():
                continue

            for file_entry in os.scandir(entry.path):
                if not file_entry.name.endswith("_extractor.yaml"):
                    continue
                extractor = LLMPromptExtractorMetadata.from_yaml(file_entry.path)

                if extractor.extractor_id in extractors:
                    raise ValueError(
                        f"Duplicate extractor ID '{extractor.extractor_id}' found in {file_entry.path}"
                    )

                extractors[extractor.extractor_id] = extractor

    return extractors


def get_extractor(extractor_id: str) -> DocumentExtractorMetadata:
    """Returns the DocumentExtractorMetadata for the given ID.

    Raises KeyError if the extractor is not found.
    """
    extractors = collect_extractors()
    if extractor_id not in extractors:
        raise KeyError(
            f"DocumentExtractorMetadata '{extractor_id}' not found. "
            f"Available extractors: {list(extractors.keys())}"
        )
    return extractors[extractor_id]


def get_extractor_for_state_and_collection(
    state_code: StateCode, collection_name: str
) -> DocumentExtractorMetadata:
    """Returns the extractor for the given state and collection.

    Raises KeyError if no matching extractor is found.
    """
    extractors = collect_extractors()
    extractor_id = f"{state_code.value}_{collection_name.upper()}"

    if extractor_id not in extractors:
        raise KeyError(
            f"No extractor found for state '{state_code.value}' and "
            f"collection '{collection_name}'. Available extractors: {list(extractors.keys())}"
        )

    return extractors[extractor_id]
