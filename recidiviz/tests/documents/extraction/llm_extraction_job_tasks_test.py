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
"""Tests for shared LLM extraction job-lifecycle tasks."""

import unittest
from unittest.mock import MagicMock

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
    LLMExtractionJobDocumentResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobDocumentTerminalFailure,
)
from recidiviz.documents.extraction.llm_extraction_job_tasks import (
    raise_on_attention_required_extraction_failures,
)

_STATE = StateCode.US_XX
_JOB_ID = "job-123"


class RaiseOnAttentionRequiredExtractionFailuresTest(unittest.TestCase):
    """Tests for raise_on_attention_required_extraction_failures."""

    def _call_with_terminal_failures(
        self, terminal_failures: list[LLMExtractionJobDocumentTerminalFailure]
    ) -> None:
        job_manager = MagicMock()
        job_manager.get_terminal_failures_with_session.return_value = terminal_failures
        raise_on_attention_required_extraction_failures(
            state_code=_STATE,
            job_id=_JOB_ID,
            job_manager=job_manager,
            session=MagicMock(),
        )

    def test_no_terminal_failures_does_not_raise(self) -> None:
        self._call_with_terminal_failures([])

    def test_only_validation_originated_failures_does_not_raise(self) -> None:
        # Validation-originated terminal failures carry no error_type, so they are
        # filtered out and do not require our attention.
        self._call_with_terminal_failures(
            [
                LLMExtractionJobDocumentTerminalFailure(
                    document_contents_id="doc_validation",
                    result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED,
                    error_type=None,
                    error_message=None,
                ),
            ]
        )

    def test_request_error_failures_raise_naming_only_those(self) -> None:
        # A validation-originated failure is mixed in but must be excluded from the
        # raised message; only the two request-error failures are attention-required.
        with self.assertRaisesRegex(
            ValueError,
            r"^Job \[job-123\] for state \[US_XX\] produced \[2\] document "
            r"failure\(s\) requiring attention: "
            r"\[doc_exhausted: DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED "
            r"\(LLM_REQUEST_RATE_LIMITED\) \[rate limited\]\], "
            r"\[doc_permanent: DOCUMENT_LEVEL_FAILURE_PERMANENT "
            r"\(LLM_REQUEST_CONTENT_FILTERED\) \[content filtered\]\]\.$",
        ):
            self._call_with_terminal_failures(
                [
                    LLMExtractionJobDocumentTerminalFailure(
                        document_contents_id="doc_exhausted",
                        result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED,
                        error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED,
                        error_message="rate limited",
                    ),
                    LLMExtractionJobDocumentTerminalFailure(
                        document_contents_id="doc_permanent",
                        result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                        error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_CONTENT_FILTERED,
                        error_message="content filtered",
                    ),
                    LLMExtractionJobDocumentTerminalFailure(
                        document_contents_id="doc_validation",
                        result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED,
                        error_type=None,
                        error_message=None,
                    ),
                ]
            )
