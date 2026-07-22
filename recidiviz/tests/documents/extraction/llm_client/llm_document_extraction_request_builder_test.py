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
"""Tests for LLMDocumentExtractionRequestBuilder."""

from typing import Any
from unittest import TestCase

from recidiviz.cloud_storage.gcs_file_system import GCSBlobDoesNotExistError
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_client.llm_document_extraction_request_builder import (
    LLMDocumentExtractionRequestBuilder,
    LLMDocumentExtractionRequestError,
)
from recidiviz.documents.extraction.llm_client.types import LLMDocumentExtractionRequest
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
)
from recidiviz.persistence.entity.operations.entities import LLMExtractionJobDocument
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

_PROJECT_ID = "recidiviz-testing"
_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "case_notes"
_INSTRUCTIONS_PROMPT = "You extract employment info.\n\n{output_instructions}"
_RESPONSE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"result": {"type": "object"}},
    "required": ["result"],
}
_REQUEST_PARAMETERS: dict[str, Any] = {
    "temperature": 0.0,
    "labels": {"state_code": "us_xx"},
}
_DOCUMENT_CONTENTS_ID = "a" * 64


def _job_document(document_contents_id: str) -> LLMExtractionJobDocument:
    return LLMExtractionJobDocument.new_with_defaults(
        state_code=_STATE_CODE.value,
        job_id="job-1",
        document_contents_id=document_contents_id,
        job_index=0,
    )


class LLMDocumentExtractionRequestBuilderTest(TestCase):
    """Tests for LLMDocumentExtractionRequestBuilder."""

    def setUp(self) -> None:
        self.fs = FakeGCSFileSystem()
        self.builder = LLMDocumentExtractionRequestBuilder(
            fs=self.fs,
            project_id=_PROJECT_ID,
            state_code=_STATE_CODE,
            collection_name=_COLLECTION_NAME,
            instructions_prompt=_INSTRUCTIONS_PROMPT,
            response_json_schema=_RESPONSE_JSON_SCHEMA,
            request_parameters=_REQUEST_PARAMETERS,
        )

    def _upload_document(self, document_contents_id: str, contents: str) -> None:
        self.fs.upload_from_string(
            path=gcs_path_for_document(
                _PROJECT_ID, _STATE_CODE, _COLLECTION_NAME, document_contents_id
            ),
            contents=contents,
            content_type="text/plain",
        )

    def test_build_request_reads_text_and_assembles_request(self) -> None:
        self._upload_document(_DOCUMENT_CONTENTS_ID, "Client started a new job.")

        request = self.builder.build_request(
            job_document=_job_document(_DOCUMENT_CONTENTS_ID)
        )

        self.assertEqual(
            LLMDocumentExtractionRequest(
                document_contents_id=_DOCUMENT_CONTENTS_ID,
                system_prompt=_INSTRUCTIONS_PROMPT,
                document_text="Client started a new job.",
                response_json_schema=_RESPONSE_JSON_SCHEMA,
                request_parameters=_REQUEST_PARAMETERS,
            ),
            request,
        )

    def test_build_request_missing_document_raises_typed_error(self) -> None:
        # No document uploaded, so the GCS read misses. It must surface as a
        # typed error carrying the document id — chained from the underlying GCS
        # error — not escape as an uncaught GCSBlobDoesNotExistError.
        with self.assertRaises(LLMDocumentExtractionRequestError) as cm:
            self.builder.build_request(
                job_document=_job_document(_DOCUMENT_CONTENTS_ID)
            )

        self.assertEqual(_DOCUMENT_CONTENTS_ID, cm.exception.document_contents_id)
        self.assertIsInstance(cm.exception.__cause__, GCSBlobDoesNotExistError)

    def test_build_request_skips_empty_document_text(self) -> None:
        # An empty document has nothing to extract, so the builder skips it by
        # returning None rather than assembling a request around empty text.
        self._upload_document(_DOCUMENT_CONTENTS_ID, "")

        self.assertIsNone(
            self.builder.build_request(
                job_document=_job_document(_DOCUMENT_CONTENTS_ID)
            )
        )
