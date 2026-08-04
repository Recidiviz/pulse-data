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
"""Builder that assembles the per-document `LLMDocumentExtractionRequest`,
including reading the document text from GCS.
"""

from typing import Any

import attr

from recidiviz.cloud_storage.gcs_file_system import (
    GCSBlobDoesNotExistError,
    GCSFileSystem,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_client.types import (
    BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME,
    LLMDocumentExtractionRequest,
)
from recidiviz.documents.extraction.models.llm_model_registry import LLMModelConfig
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
)
from recidiviz.persistence.entity.operations.entities import LLMExtractionJobDocument


class LLMDocumentExtractionRequestError(Exception):
    """A document could not be assembled into an extraction request — e.g. the
    document_contents_id was not found in GCS.
    """

    def __init__(self, *, document_contents_id: str, message: str) -> None:
        self.document_contents_id = document_contents_id
        super().__init__(message)


@attr.define(frozen=True, kw_only=True)
class LLMDocumentExtractionRequestBuilder:
    """Assembles the per-document `LLMDocumentExtractionRequest`."""

    fs: GCSFileSystem = attr.ib()
    """The GCS file system the document text is read from."""

    project_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The project whose document blob storage bucket holds the text."""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state whose documents this job processes."""

    collection_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The document collection the job's documents belong to."""

    instructions_prompt: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The fully-rendered system prompt (instructions + output format) sent with
    every request for this extractor."""

    response_json_schema: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The generated standard-JSON-Schema dict, destined for the request's
    response_json_schema."""

    request_parameters: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The generation parameters — temperature, thinking budget, caching, billing
    labels — keyed by parameter name."""

    @staticmethod
    def build_request_parameters(
        *, model_config: LLMModelConfig, labels: dict[str, str]
    ) -> dict[str, Any]:
        """Returns the request_parameters for an extractor: |model_config|'s
        resolved generation parameters (temperature, thinking budget, etc.) plus
        the billing |labels| for cost attribution, keyed by parameter name.

        The billing-labels key is always present (mapped to |labels|, empty or
        not), never omitted.
        """
        return {
            **model_config.resolved_parameter_values,
            BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME: labels,
        }

    def build_request(
        self, *, job_document: LLMExtractionJobDocument
    ) -> LLMDocumentExtractionRequest | None:
        """Returns the extraction request for |job_document|, reading its text
        from GCS, or None if the document's text is empty (nothing to extract).

        Raises `LLMDocumentExtractionRequestError` when the document_contentds_id is not found in GCS.
        """
        document_contents_id = job_document.document_contents_id
        path = gcs_path_for_document(
            self.project_id,
            self.state_code,
            self.collection_name,
            document_contents_id,
        )
        try:
            document_text = self.fs.download_as_string(path)
        except GCSBlobDoesNotExistError as e:
            raise LLMDocumentExtractionRequestError(
                document_contents_id=document_contents_id,
                message=f"Document text for [{document_contents_id}] not found at "
                f"[{path.uri()}].",
            ) from e

        if not document_text:
            return None

        return LLMDocumentExtractionRequest(
            document_contents_id=document_contents_id,
            system_prompt=self.instructions_prompt,
            document_text=document_text,
            response_json_schema=self.response_json_schema,
            request_parameters=self.request_parameters,
        )
