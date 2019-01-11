# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Tests for converting sentences."""
import unittest

from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import sentence


class SentenceConverterTest(unittest.TestCase):
    """Tests for converting sentences."""

    def testParseSentence(self):
        # Arrange
        ingest_sentence = ingest_info_pb2.Sentence(
            sentence_id='SENTENCE_ID',
            min_length='1',
            post_release_supervision_length=''
        )

        # Act
        result = sentence.convert(ingest_sentence)

        # Assert
        expected_result = entities.Sentence.new_with_defaults(
            external_id='SENTENCE_ID',
            min_length_days=1,
            post_release_supervision_length_days=0
        )

        self.assertEqual(result, expected_result)
