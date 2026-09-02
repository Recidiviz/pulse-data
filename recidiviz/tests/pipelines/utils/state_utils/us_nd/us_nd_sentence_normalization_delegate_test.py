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
"""Tests for UsNdSentenceNormalizationDelegate."""
import unittest

from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_sentence_normalization_delegate import (
    UsNdSentenceNormalizationDelegate,
)


class TestUsNdSentenceNormalizationDelegate(unittest.TestCase):
    """Tests for UsNdSentenceNormalizationDelegate.

    The behavior of the IMPOSED_PENDING_SERVING inference itself is covered in
    normalize_sentences_test.py; this asserts that US_ND opts into it, which is
    what makes queued consecutive sentences visible downstream.
    """

    def test_infers_imposed_pending_serving_from_imposed_date(self) -> None:
        self.assertTrue(
            UsNdSentenceNormalizationDelegate().infer_imposed_pending_serving_from_imposed_date
        )
