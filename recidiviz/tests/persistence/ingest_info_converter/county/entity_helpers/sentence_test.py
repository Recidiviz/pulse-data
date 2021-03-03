# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
import datetime
import unittest

from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.county.entity_helpers import sentence


class SentenceConverterTest(unittest.TestCase):
    """Tests for converting sentences."""

    def testParseSentence(self):
        # Arrange
        ingest_sentence = ingest_info_pb2.Sentence(
            sentence_id="SENTENCE_ID",
            min_length="1",
            date_imposed="2000-1-1",
            post_release_supervision_length="",
        )

        # Act
        sentence_builder = entities.Sentence.builder()
        sentence.copy_fields_to_builder(
            sentence_builder,
            ingest_sentence,
            IngestMetadata.new_with_defaults(
                ingest_time=datetime.datetime(year=2018, month=6, day=8)
            ),
        )
        result = sentence_builder.build()

        # Assert
        expected_result = entities.Sentence.new_with_defaults(
            external_id="SENTENCE_ID",
            min_length_days=1,
            post_release_supervision_length_days=0,
            date_imposed=datetime.date(year=2000, month=1, day=1),
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(result, expected_result)

    def testCompletionDateInFuture(self):
        # Arrange
        ingest_sentence = ingest_info_pb2.Sentence(
            sentence_id="SENTENCE_ID",
            completion_date="2020-1-1",
        )

        # Act
        sentence_builder = entities.Sentence.builder()
        sentence.copy_fields_to_builder(
            sentence_builder,
            ingest_sentence,
            IngestMetadata.new_with_defaults(
                ingest_time=datetime.datetime(year=2018, month=6, day=8)
            ),
        )
        result = sentence_builder.build()

        # Assert
        expected_result = entities.Sentence.new_with_defaults(
            external_id="SENTENCE_ID",
            completion_date=None,
            projected_completion_date=datetime.date(year=2020, month=1, day=1),
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(result, expected_result)

    def testParseSentence_completionDateWithoutStatus_marksAsCompleted(self):
        # Arrange
        ingest_sentence = ingest_info_pb2.Sentence(
            sentence_id="SENTENCE_ID",
            completion_date="2018-1-1",
        )

        # Act
        sentence_builder = entities.Sentence.builder()
        sentence.copy_fields_to_builder(
            sentence_builder,
            ingest_sentence,
            IngestMetadata.new_with_defaults(
                ingest_time=datetime.datetime(year=2018, month=6, day=8)
            ),
        )
        result = sentence_builder.build()

        # Assert
        expected_result = entities.Sentence.new_with_defaults(
            external_id="SENTENCE_ID",
            completion_date=datetime.date(year=2018, month=1, day=1),
            status=SentenceStatus.COMPLETED,
        )

        self.assertEqual(result, expected_result)
