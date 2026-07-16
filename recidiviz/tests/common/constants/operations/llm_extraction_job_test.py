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
"""Tests for constants in
recidiviz/common/constants/operations/llm_extraction_job.py."""

import unittest

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)


class LLMExtractionJobDocumentResultTypeTest(unittest.TestCase):
    """Tests full enum coverage of the classification helpers on
    LLMExtractionJobDocumentResultType."""

    def test_is_success_result_type_all_enums(self) -> None:
        # Every member must be classified — an unenumerated member raises.
        for result_type in LLMExtractionJobDocumentResultType:
            expected = result_type is LLMExtractionJobDocumentResultType.SUCCESS
            self.assertEqual(
                expected,
                LLMExtractionJobDocumentResultType.is_success_result_type(result_type),
            )

    def test_is_terminal_result_type_all_enums(self) -> None:
        terminal = {
            LLMExtractionJobDocumentResultType.SUCCESS,
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED,
        }
        # Every member must be classified — an unenumerated member raises.
        for result_type in LLMExtractionJobDocumentResultType:
            self.assertEqual(
                result_type in terminal,
                LLMExtractionJobDocumentResultType.is_terminal_result_type(result_type),
            )
