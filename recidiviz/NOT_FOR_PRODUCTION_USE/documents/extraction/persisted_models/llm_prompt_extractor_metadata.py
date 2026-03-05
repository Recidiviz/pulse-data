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
"""LLMPromptExtractorMetadata model class.
This is a pure data class that handles persistence (reading/writing to BQ metadata
tables). Business logic like prompt rendering and request building lives in
LLMExtractionRequest in llm_client.py.
"""
import datetime
import hashlib
import os
from pathlib import Path
from typing import Any

import attr
import pytz

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.dataset_config import (
    EXTRACTION_METADATA_DATASET_ID,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_collection_metadata import (
    DocumentExtractorCollectionMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_metadata import (
    DocumentExtractorMetadata,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.yaml_dict import YAMLDict

# Providers supported by the LiteLLM Batch API
SUPPORTED_LLM_PROVIDERS = frozenset(
    {"vertex_ai", "openai", "azure", "bedrock", "hosted_vllm"}
)


@attr.define(frozen=True)
class LLMPromptExtractorMetadata(DocumentExtractorMetadata):
    """Extractor that uses an LLM with a specific prompt to extract information.
    This is a pure data class - all fields map directly to metadata table columns.
    Business logic lives in LLMExtractionRequest in llm_client.py.
    """

    # The fully-rendered instruction prompt with output_format_instructions baked in.
    # This is the complete system message sent to the LLM (minus per-document content).
    instructions_prompt: str

    # The LLM provider to use (e.g., "vertex_ai", "openai", "azure", "bedrock")
    llm_provider: str

    # The model that should be used (e.g., "gemini-2.5-flash")
    model: str

    def __attrs_post_init__(self) -> None:
        if self.llm_provider not in SUPPORTED_LLM_PROVIDERS:
            raise ValueError(
                f"llm_provider must be one of {SUPPORTED_LLM_PROVIDERS}, "
                f"got: {self.llm_provider}"
            )
        if "/" in self.model:
            raise ValueError(f"model must not contain '/': {self.model}")

    def instructions_prompt_hash(self) -> str:
        """SHA256 hash of the instructions prompt. Used to build the unique id
        for this extractor version.
        """
        return hashlib.sha256(self.instructions_prompt.encode()).hexdigest()[:16]

    def extractor_version_id(self) -> str:
        """ID unique to this extractor which will change if the prompt or model
        changes.
        """
        return f"{self.extractor_id}_{self.llm_provider}_{self.model.replace('-', '_')}_{self.instructions_prompt_hash()}"

    @classmethod
    def metadata_table_address(
        cls, sandbox_dataset_prefix: str | None = None
    ) -> BigQueryAddress:
        dataset_id = EXTRACTION_METADATA_DATASET_ID
        if sandbox_dataset_prefix:
            dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_dataset_prefix, dataset_id
            )
        return BigQueryAddress(
            dataset_id=dataset_id,
            table_id="llm_prompt_extractors",
        )

    def as_metadata_row(self) -> dict[str, Any]:
        return {
            # TODO(#61742): Make constants for all these column names
            "extractor_id": self.extractor_id,
            "extractor_version_id": self.extractor_version_id(),
            "state_code": self.state_code.value,
            "collection_name": self.collection_name,
            "description": self.description,
            "input_document_collection_name": self.input_document_collection_name,
            "llm_provider": self.llm_provider,
            "model": self.model,
            "instructions_prompt": self.instructions_prompt,
            "instructions_prompt_hash": self.instructions_prompt_hash(),
            "update_datetime": datetime.datetime.now(tz=pytz.UTC),
        }

    @classmethod
    def from_metadata_row(cls, row: dict[str, Any]) -> "LLMPromptExtractorMetadata":
        """Reconstructs an LLMPromptExtractor from a metadata table row."""
        return cls(
            state_code=StateCode(row["state_code"]),
            extractor_id=row["extractor_id"],
            collection_name=row["collection_name"],
            description=row["description"],
            input_document_collection_name=row["input_document_collection_name"],
            instructions_prompt=row["instructions_prompt"],
            llm_provider=row["llm_provider"],
            model=row["model"],
        )

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "LLMPromptExtractorMetadata":
        """Loads an LLMPromptExtractor from a YAML file.

        Expected path structure:
            collections/llm_prompt/<collection_name>/<state_code>_extractor.yaml

        For example:
            collections/llm_prompt/case_note_employment_info/us_ix_extractor.yaml
        - State code derived from filename prefix (us_ix_extractor.yaml -> US_IX)
        - Collection name derived from parent directory (case_note_employment_info -> CASE_NOTE_EMPLOYMENT_INFO)

        The prompt is built from the collection's prompt_template.txt with
        optional prompt_vars substituted in. Then {output_format_instructions}
        is rendered from the collection's output schema at load time, producing
        the final instructions_prompt.

        Args:
            yaml_path: Path to the YAML configuration file
        """
        path = Path(yaml_path)
        yaml_dict = YAMLDict.from_path(yaml_path)

        # Derive state_code from filename (e.g., us_ix_extractor.yaml -> US_IX)
        filename_stem = path.stem  # e.g., "us_ix_extractor"
        state_code_str = filename_stem.replace("_extractor", "").upper()
        state_code = StateCode(state_code_str)

        # Derive collection_name from parent directory
        collection_dir = path.parent
        collection_name = collection_dir.name.upper()

        # Get collection_name from YAML and validate it matches the directory
        yaml_collection_name = yaml_dict.pop("collection_name", str).upper()
        if yaml_collection_name != collection_name:
            raise ValueError(
                f"Extractor config collection_name '{yaml_collection_name}' does not "
                f"match parent directory name '{collection_name}' in {yaml_path}"
            )

        extractor_id = f"{state_code.value}_{collection_name}"

        # Load the collection's prompt_template.txt and substitute prompt_vars
        template_path = os.path.join(collection_dir, "prompt_template.txt")
        if not os.path.exists(template_path):
            raise ValueError(
                f"No prompt_template.txt found at {template_path}. "
                f"Each collection directory must contain a prompt_template.txt."
            )
        with open(template_path, "r", encoding="utf-8") as f:
            base_template = f.read()

        prompt_vars = yaml_dict.pop_optional("prompt_vars", dict) or {}

        # Load the collection's output schema to render output_format_instructions
        collection_yaml_path = os.path.join(collection_dir, "collection.yaml")
        collection = DocumentExtractorCollectionMetadata.from_yaml(collection_yaml_path)
        output_format_instructions = (
            collection.output_schema.output_format_instructions()
        )

        instructions_prompt = StrictStringFormatter().format(
            base_template,
            output_format_instructions=output_format_instructions,
            **prompt_vars,
        )

        return cls(
            state_code=state_code,
            extractor_id=extractor_id,
            collection_name=collection_name,
            description=yaml_dict.pop("description", str),
            input_document_collection_name=yaml_dict.pop(
                "input_document_collection_name", str
            ).upper(),
            instructions_prompt=instructions_prompt,
            llm_provider=yaml_dict.pop("llm_provider", str),
            model=yaml_dict.pop("model", str),
        )
