# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for entity_hydration_utils.py."""

import unittest
from datetime import date
from typing import Any, Dict, List, Tuple

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from recidiviz.calculator.pipeline.utils.beam_utils import entity_hydration_utils
from recidiviz.calculator.pipeline.utils.beam_utils.bigquery_io_utils import (
    ConvertDictToKVTuple,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    UsMoIncarcerationSentence,
    UsMoSentenceStatus,
    UsMoSupervisionSentence,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.state.entities import (
    SentenceType,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionSentence,
)


class TestCovertSentenceToStateSpecificType(unittest.TestCase):
    """Tests the ConvertSentencesToStateSpecificType DoFn."""

    TEST_PERSON_ID = 456

    TEST_MO_SENTENCE_STATUS_ROWS = [
        {
            "person_id": TEST_PERSON_ID,
            "sentence_external_id": "123-external-id",
            "sentence_status_external_id": "123-external-id-1",
            "status_code": "10I1000",
            "status_date": "20171012",
            "status_description": "New Court Comm-Institution",
        }
    ]

    TEST_CONVERTED_MO_STATUS = UsMoSentenceStatus(
        sentence_status_external_id="123-external-id-1",
        sentence_external_id="123-external-id",
        status_code="10I1000",
        status_date=date(2017, 10, 12),
        status_description="New Court Comm-Institution",
    )

    @staticmethod
    def convert_sentence_output_is_valid(expected_output: List[SentenceType]):
        """Beam assert matcher for checking output of ConvertSentencesToStateSpecificType."""

        def _convert_sentence_output_is_valid(
            output: List[Tuple[int, Dict[str, List[Any]]]]
        ):
            if not expected_output:
                raise ValueError("Must supply expected_output to validate against.")

            for _, entities in output:
                if isinstance(expected_output[0], StateSupervisionSentence):
                    sentence_output = entities[StateSupervisionSentence.__name__]
                else:
                    sentence_output = entities[StateIncarcerationSentence.__name__]

                if len(sentence_output) != len(expected_output):
                    raise ValueError(
                        f"Expected output length [{len(expected_output)}] != output length [{len(sentence_output)}]"
                    )
                for i, sentence in enumerate(sentence_output):
                    expected_sentence = expected_output[i]

                    if not isinstance(sentence, type(expected_sentence)):
                        raise ValueError(
                            f"sentence is not instance of [{type(expected_sentence)}]"
                        )
                    if sentence != expected_sentence:
                        raise ValueError(
                            f"sentence [{sentence}] != expected_sentence [{expected_sentence}]"
                        )

        return _convert_sentence_output_is_valid

    def test_ConvertSentenceToStateSpecificType_incarceration_sentence_fake_state_not_mo(
        self,
    ):
        """Tests that the sentence does not get converted to the state_specific_type for states where that is not
        defined."""
        incarceration_sentence_id = 123

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=incarceration_sentence_id,
            state_code="US_XX",
            external_id="123-external-id",
            start_date=date(2000, 1, 1),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.run_test_pipeline(
            self.TEST_PERSON_ID,
            incarceration_sentence,
            self.TEST_MO_SENTENCE_STATUS_ROWS,
            incarceration_sentence,
        )

    def test_ConvertSentenceToStateSpecificType_incarceration_sentence_mo(self):
        """Tests that for MO, incarceration sentences get converted to UsMoIncarcerationSentence."""
        incarceration_sentence_id = 123

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=incarceration_sentence_id,
            state_code="US_MO",
            external_id="123-external-id",
            start_date=date(2000, 1, 1),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        expected_sentence = UsMoIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=incarceration_sentence_id,
            state_code="US_MO",
            external_id="123-external-id",
            start_date=date(2000, 1, 1),
            base_sentence=incarceration_sentence,
            sentence_statuses=[self.TEST_CONVERTED_MO_STATUS],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.run_test_pipeline(
            self.TEST_PERSON_ID,
            incarceration_sentence,
            self.TEST_MO_SENTENCE_STATUS_ROWS,
            expected_sentence,
        )

    def test_ConvertSentenceToStateSpecificType_supervision_sentence_mo(self):
        """Tests that for MO, supervision sentences get converted to UsMoSupervisionSentence."""
        supervision_sentence_id = 123

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=supervision_sentence_id,
            state_code="US_MO",
            external_id="123-external-id",
            start_date=date(2000, 1, 1),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        expected_sentence = UsMoSupervisionSentence.new_with_defaults(
            supervision_sentence_id=supervision_sentence_id,
            state_code="US_MO",
            external_id="123-external-id",
            start_date=date(2000, 1, 1),
            base_sentence=supervision_sentence,
            sentence_statuses=[self.TEST_CONVERTED_MO_STATUS],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.run_test_pipeline(
            self.TEST_PERSON_ID,
            supervision_sentence,
            self.TEST_MO_SENTENCE_STATUS_ROWS,
            expected_sentence,
        )

    # TODO(#4375): Update tests to run actual pipeline code and only mock BQ I/O
    def run_test_pipeline(
        self,
        person_id: int,
        sentence: SentenceType,
        us_mo_sentence_status_rows: List[Dict[str, str]],
        expected_sentence: SentenceType,
    ):
        """Runs a test pipeline to test ConvertSentencesToStateSpecificType and checks
        the output against expected."""
        person = StatePerson.new_with_defaults(
            state_code=sentence.state_code, person_id=person_id
        )

        test_pipeline = TestPipeline()

        us_mo_sentence_statuses = (
            test_pipeline
            | "Create MO sentence statuses" >> beam.Create(us_mo_sentence_status_rows)
        )

        sentence_status_rankings_as_kv = (
            us_mo_sentence_statuses
            | "Convert MO sentence status ranking table to KV tuples"
            >> beam.ParDo(ConvertDictToKVTuple(), "person_id")
        )

        people = test_pipeline | "Create person_id person tuple" >> beam.Create(
            [(person_id, person)]
        )

        sentences = test_pipeline | "Create person_id sentence tuple" >> beam.Create(
            [(person_id, sentence)]
        )

        empty_sentences = test_pipeline | "Create empty PCollection" >> beam.Create([])

        if isinstance(sentence, StateSupervisionSentence):
            supervision_sentences = sentences
            incarceration_sentences = empty_sentences
        else:
            incarceration_sentences = sentences
            supervision_sentences = empty_sentences

        entities_and_statuses = (
            {
                StatePerson.__name__: people,
                StateIncarcerationSentence.__name__: incarceration_sentences,
                StateSupervisionSentence.__name__: supervision_sentences,
                US_MO_SENTENCE_STATUSES_VIEW_NAME: sentence_status_rankings_as_kv,
            }
            | "Group sentences to the sentence statuses for that person"
            >> beam.CoGroupByKey()
        )

        output = (
            entities_and_statuses
            | "Convert to state-specific sentences"
            >> beam.ParDo(
                entity_hydration_utils.ConvertEntitiesToStateSpecificTypes(),
                state_code=person.state_code,
            )
        )

        # Expect no change
        expected_output = [expected_sentence]

        assert_that(
            output,
            self.convert_sentence_output_is_valid(expected_output),
        )

        test_pipeline.run()
