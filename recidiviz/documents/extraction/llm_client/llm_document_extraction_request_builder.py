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
"""Builder that assembles the per-document `LLMDocumentExtractionRequest` for any
`DocumentTextSource`, or directly from text the caller already holds. Also
defines the error the builder raises, and the GCS-backed `DocumentTextSource`
the pipeline path uses.
"""

from typing import Any

import attr

from recidiviz.cloud_storage.gcs_file_system import (
    GCSBlobDoesNotExistError,
    GCSFileSystem,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.document_text_source import DocumentTextSource
from recidiviz.documents.extraction.llm_client.types import (
    BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME,
    LLMDocumentExtractionRequest,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import LLMModelConfig
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
)


class LLMDocumentExtractionRequestError(Exception):
    """A document could not be assembled into an extraction request — e.g. the
    document_contents_id was not found in GCS.
    """

    def __init__(self, *, document_contents_id: str, message: str) -> None:
        self.document_contents_id = document_contents_id
        super().__init__(message)


@attr.define(frozen=True, kw_only=True)
class LLMDocumentExtractionRequestBuilder:
    """Assembles the per-document `LLMDocumentExtractionRequest` for any
    `DocumentTextSource`, or directly from text the caller already holds.
    """

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
        self, *, document: DocumentTextSource
    ) -> LLMDocumentExtractionRequest | None:
        """Returns the extraction request for |document|, fetching its text through
        the document's own fetch, or None if the text is empty (nothing to extract).

        Raises `LLMDocumentExtractionRequestError` when the document's text cannot
        be fetched.
        """
        document_text = document.fetch_document_text()
        if not document_text:
            return None

        return self.build_request_for_text(
            document_contents_id=document.document_contents_id,
            document_text=document_text,
        )

    def build_request_for_text(
        self, *, document_contents_id: str, document_text: str
    ) -> LLMDocumentExtractionRequest:
        """Returns the extraction request for |document_text|.

        Raises `LLMDocumentExtractionRequestError` when the text is empty.
        """
        if not document_text:
            raise LLMDocumentExtractionRequestError(
                document_contents_id=document_contents_id,
                message=f"Cannot build an extraction request for document "
                f"[{document_contents_id}], whose text is empty.",
            )

        return LLMDocumentExtractionRequest(
            document_contents_id=document_contents_id,
            system_prompt=self.instructions_prompt,
            document_text=document_text,
            response_json_schema=self.response_json_schema,
            request_parameters=self.request_parameters,
        )

    @classmethod
    def for_config(
        cls,
        *,
        config: LLMExtractorConfig,
        billing_labels: dict[str, str],
    ) -> "LLMDocumentExtractionRequestBuilder":
        """Returns the builder for |config|: its rendered instructions prompt, its
        collection's generated JSON schema, and the request parameters its model
        config resolves to.
        """
        return cls(
            instructions_prompt=config.instructions_prompt,
            response_json_schema=config.extractor_collection.generate_json_schema(),
            request_parameters=cls.build_request_parameters(
                model_config=config.model_config, labels=billing_labels
            ),
        )


@attr.define(frozen=True, kw_only=True)
class GCSDocumentTextSource:
    """A `DocumentTextSource` whose text is read from a document collection's
    GCS blob storage on each fetch.
    """

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier of the document, which names its text blob in GCS."""

    fs: GCSFileSystem = attr.ib()
    """The GCS file system the document text is read from."""

    project_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The project whose document blob storage bucket holds the text."""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state the document belongs to."""

    collection_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The document collection the document belongs to."""

    source_sandbox_prefix: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """When set, document text is read from the state's sandbox blob-storage bucket,
    namespaced by this prefix. None in production."""

    def fetch_document_text(self) -> str:
        """Returns the document's full text, read from GCS.

        Raises `LLMDocumentExtractionRequestError` when the document_contents_id is
        not found in GCS.
        """
        path = gcs_path_for_document(
            project_id=self.project_id,
            state_code=self.state_code,
            collection_name=self.collection_name,
            document_contents_id=self.document_contents_id,
            sandbox_prefix=self.source_sandbox_prefix,
        )
        try:
            return self.fs.download_as_string(path)
        except GCSBlobDoesNotExistError as e:
            raise LLMDocumentExtractionRequestError(
                document_contents_id=self.document_contents_id,
                message=f"Document text for [{self.document_contents_id}] not found "
                f"at [{path.uri()}].",
            ) from e
