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
"""Configuration class for extractor collections."""
import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

import attr
import pytz

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.dataset_config import (
    EXTRACTION_METADATA_DATASET_ID,
)
from recidiviz.utils.yaml_dict import YAMLDict

DEFAULT_LLM_CONFIDENCE_THRESHOLD: float = 0.8


@attr.define(frozen=True)
class DocumentExtractorCollectionMetadata:
    """Metadata about a collection of extractors (e.g. prompts) that have a single aim
    and shared output schema but may extract info from different collections / subsets
    of documents.
    """

    # E.g. EMPLOYMENT or HOUSING or CASE_NOTE_EXTRACTED_EMPLOYER. This is unique across
    # all configured collections.
    name: str

    # Explains the aim of this class of prompts. E.g. "Extract info about a person's
    # housing status, including whether they are housed, whether that housing is stable,
    # and their home address"
    description: str

    # The output schema that all results from any Extractor in this collection should
    # have. This can change over time, but we must enforce that at any point in time,
    # the most current version of every prompt of a given collection has the same output
    # schema so that we can write to unified, state-agnostic tables.
    output_schema: ExtractionOutputSchema

    # Minimum confidence score (0.0-1.0) required for a row to appear in the
    # validated output view. If ANY field in a row has confidence below this
    # threshold, the entire row is excluded from validated results.
    # TODO(#61701): Revisit per-field confidence thresholds
    confidence_threshold: float

    @classmethod
    def metadata_table_address(
        cls, sandbox_dataset_prefix: str | None
    ) -> BigQueryAddress:
        dataset_id = EXTRACTION_METADATA_DATASET_ID
        if sandbox_dataset_prefix:
            dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_dataset_prefix, dataset_id
            )
        return BigQueryAddress(
            dataset_id=dataset_id,
            table_id="extractor_collections",
        )

    def as_metadata_row(self) -> dict[str, Any]:
        """Row that will be written to a metadata table about extractor collections
        every time anything about the collection changes (e.g. description or schema
        changes).
        """
        return {
            "name": self.name,
            "description": self.description,
            "output_schema_json": json.dumps(self.output_schema.to_llm_json_schema()),
            "schema_hash": self.schema_hash(),
            "update_datetime": datetime.datetime.now(tz=pytz.UTC),
        }

    def schema_hash(self) -> str:
        """Returns a hash of the output schema for versioning purposes."""
        schema_str = json.dumps(self.output_schema.to_llm_json_schema(), sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "DocumentExtractorCollectionMetadata":
        """Loads an ExtractorCollection from a YAML file.

        Expected path structure:
            collections/llm_prompt/<collection_name>/collection.yaml

        The collection name is derived from the parent directory name (uppercased)
        and validated against the 'name' field in the YAML.
        """
        path = Path(yaml_path)
        yaml_dict = YAMLDict.from_path(yaml_path)

        # Derive name from parent directory
        dir_name = path.parent.name.upper()

        # Validate against YAML name field
        yaml_name = yaml_dict.pop("name", str).upper()
        if yaml_name != dir_name:
            raise ValueError(
                f"Collection YAML name '{yaml_name}' does not match parent "
                f"directory name '{dir_name}' in {yaml_path}"
            )

        description = yaml_dict.pop("description", str)

        output_schema_yaml = yaml_dict.pop_dict("output_schema")
        output_schema = ExtractionOutputSchema.from_yaml_dict(
            output_schema_yaml,
            collection_description=description,
        )
        confidence_threshold = yaml_dict.pop_optional("confidence_threshold", float)

        return cls(
            name=dir_name,
            description=description,
            output_schema=output_schema,
            confidence_threshold=confidence_threshold
            if confidence_threshold is not None
            else DEFAULT_LLM_CONFIDENCE_THRESHOLD,
        )
