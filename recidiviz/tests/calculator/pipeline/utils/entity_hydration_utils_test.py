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
"""Tests for entity_hydration_utils.py."""

import unittest
from datetime import date
from typing import List, Tuple, Dict

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline

from recidiviz.calculator.pipeline.utils import entity_hydration_utils
from recidiviz.calculator.pipeline.utils.beam_utils import ConvertDictToKVTuple
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import \
    UsMoIncarcerationSentence, UsMoSentenceStatus, UsMoSupervisionSentence
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseType
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, \
    StateSupervisionViolationResponse, StateSupervisionViolation, \
    StateSupervisionViolationTypeEntry, StateIncarcerationSentence, StateCharge, StateSupervisionSentence, \
    StateSentenceGroup, SentenceType


class TestCovertSentenceToStateSpecificType(unittest.TestCase):
    """Tests the ConvertSentencesToStateSpecificType DoFn."""

    TEST_PERSON_ID = 456

    TEST_MO_SENTENCE_STATUS_ROWS = [
        {'person_id': TEST_PERSON_ID, 'sentence_external_id': '123-external-id',
         'sentence_status_external_id': '123-external-id-1', 'status_code': '10I1000', 'status_date': '20171012',
         'status_description': 'New Court Comm-Institution'}
    ]

    TEST_CONVERTED_MO_STATUS = UsMoSentenceStatus(
        sentence_status_external_id='123-external-id-1',
        sentence_external_id='123-external-id',
        status_code='10I1000',
        status_date=date(2017, 10, 12),
        status_description='New Court Comm-Institution'
    )

    @staticmethod
    def convert_sentence_output_is_valid(expected_output: List[Tuple[int, SentenceType]]):
        """Beam assert matcher for checking output of ConvertSentencesToStateSpecificType."""

        def _convert_sentence_output_is_valid(output: List[Tuple[int, SentenceType]]):
            if len(output) != len(expected_output):
                raise ValueError(
                    f'Expected output length [{len(expected_output)}] != output length [{len(output)}]')
            for i, (person_id, sentence) in enumerate(output):
                expected_person_id, expected_sentence = expected_output[i]
                if person_id != expected_person_id:
                    raise ValueError(f'person_id [{person_id}] != expected_person_id [{expected_person_id}]')
                if not isinstance(sentence, type(expected_sentence)):
                    raise ValueError(f'sentence is not instance of [{type(expected_person_id)}]')
                if sentence != expected_sentence:
                    raise ValueError(f'sentence [{sentence}] != expected_sentence [{expected_sentence}]')

        return _convert_sentence_output_is_valid

    def test_ConvertSentenceToStateSpecificType_incarceration_sentence_fake_state_not_mo(self):
        """Tests that the sentence does not get converted to the state_specific_type for states where that is not
        defined."""
        incarceration_sentence_id = 123

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=incarceration_sentence_id,
            state_code='US_XX',
            external_id='123-external-id',
            start_date=date(2000, 1, 1),
        )

        self.run_test_pipeline(self.TEST_PERSON_ID,
                               incarceration_sentence,
                               self.TEST_MO_SENTENCE_STATUS_ROWS,
                               incarceration_sentence)

    def test_ConvertSentenceToStateSpecificType_incarceration_sentence_mo(self):
        """Tests that for MO, incarceration sentences get converted to UsMoIncarcerationSentence."""
        incarceration_sentence_id = 123

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=incarceration_sentence_id,
            state_code='US_MO',
            external_id='123-external-id',
            start_date=date(2000, 1, 1),
        )

        expected_sentence = UsMoIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=incarceration_sentence_id,
            state_code='US_MO',
            external_id='123-external-id',
            start_date=date(2000, 1, 1),
            base_sentence=incarceration_sentence,
            sentence_statuses=[self.TEST_CONVERTED_MO_STATUS]
        )

        self.run_test_pipeline(self.TEST_PERSON_ID,
                               incarceration_sentence,
                               self.TEST_MO_SENTENCE_STATUS_ROWS,
                               expected_sentence)

    def test_ConvertSentenceToStateSpecificType_supervision_sentence_mo(self):
        """Tests that for MO, supervision sentences get converted to UsMoSupervisionSentence."""
        supervision_sentence_id = 123

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=supervision_sentence_id,
            state_code='US_MO',
            external_id='123-external-id',
            start_date=date(2000, 1, 1),
        )

        expected_sentence = UsMoSupervisionSentence.new_with_defaults(
            supervision_sentence_id=supervision_sentence_id,
            state_code='US_MO',
            external_id='123-external-id',
            start_date=date(2000, 1, 1),
            base_sentence=supervision_sentence,
            sentence_statuses=[self.TEST_CONVERTED_MO_STATUS]
        )

        self.run_test_pipeline(self.TEST_PERSON_ID,
                               supervision_sentence,
                               self.TEST_MO_SENTENCE_STATUS_ROWS,
                               expected_sentence)

    # TODO(#4375): Update tests to run actual pipeline code and only mock BQ I/O
    def run_test_pipeline(self,
                          person_id: int,
                          sentence: SentenceType,
                          us_mo_sentence_status_rows: List[Dict[str, str]],
                          expected_sentence: SentenceType):
        """Runs a test pipeline to test ConvertSentencesToStateSpecificType and checks the output against expected."""
        test_pipeline = TestPipeline()

        us_mo_sentence_statuses = (
            test_pipeline | 'Create MO sentence statuses' >> beam.Create(us_mo_sentence_status_rows)
        )

        sentence_status_rankings_as_kv = (
            us_mo_sentence_statuses |
            'Convert MO sentence status ranking table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(), 'person_id')
        )

        sentences = (test_pipeline
                     | 'Create person_id sentence tuple' >>
                     beam.Create([(person_id, sentence)])
                     )

        empty_sentences = (test_pipeline
                           | 'Create empty PCollection' >>
                           beam.Create([]))

        if isinstance(sentence, StateSupervisionSentence):
            supervision_sentences = sentences
            incarceration_sentences = empty_sentences
        else:
            incarceration_sentences = sentences
            supervision_sentences = empty_sentences

        sentences_and_statuses = (
            {'incarceration_sentences': incarceration_sentences,
             'supervision_sentences': supervision_sentences,
             'sentence_statuses': sentence_status_rankings_as_kv}
            | 'Group sentences to the sentence statuses for that person' >>
            beam.CoGroupByKey()
        )

        output = (
            sentences_and_statuses
            | 'Convert to state-specific sentences' >>
            beam.ParDo(
                entity_hydration_utils.ConvertSentencesToStateSpecificType()).with_outputs('incarceration_sentences',
                                                                                           'supervision_sentences')
        )

        # Expect no change
        expected_output = [(person_id, expected_sentence)]

        if isinstance(sentence, StateSupervisionSentence):
            assert_that(output.supervision_sentences, self.convert_sentence_output_is_valid(expected_output))
        else:
            assert_that(output.incarceration_sentences, self.convert_sentence_output_is_valid(expected_output))

        test_pipeline.run()


class TestSetViolationResponseOnIncarcerationPeriod(unittest.TestCase):
    """Tests the SetViolationResponseOnIncarcerationPeriod DoFn."""
    def testSetViolationResponseOnIncarcerationPeriod(self):
        """Tests that the hydrated StateSupervisionViolationResponse is set
        on the StateIncarcerationPeriod."""
        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_XX',
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION
            )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            admission_date=date(2015, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            PROBATION_REVOCATION,
            release_date=date(2020, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED,
            source_supervision_violation_response=supervision_violation_response
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code='US_XX',
            supervision_violation_id=55555
        )

        hydrated_supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_XX',
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION,
                supervision_violation=supervision_violation
            )

        incarceration_periods_violation_responses = {
            'incarceration_periods': [incarceration_period],
            'violation_responses': [
                hydrated_supervision_violation_response
            ]}

        expected_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_XX',
                admission_date=date(2015, 5, 30),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                PROBATION_REVOCATION,
                release_date=date(2020, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED,
                source_supervision_violation_response=
                hydrated_supervision_violation_response
            )

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(12345,
                                  incarceration_periods_violation_responses)])
                  | 'Set Supervision Violation Response on '
                    'Incarceration Period' >>
                  beam.ParDo(
                      entity_hydration_utils.
                      SetViolationResponseOnIncarcerationPeriod())
                  )

        assert_that(output, equal_to([(12345, expected_incarceration_period)]))

        test_pipeline.run()


class TestSetViolationOnViolationsResponse(unittest.TestCase):
    """Tests the SetViolationOnViolationsResponse DoFn."""
    def testSetViolationOnViolationsResponse(self):
        """Tests that the hydrated StateSupervisionViolation is set
        on the StateSupervisionViolationResponse."""

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_XX',
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION
            )

        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                state_code='US_XX',
                supervision_violation_id=999,
                supervision_violation_responses=
                [supervision_violation_response],
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code='US_XX',
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )]
            )

        supervision_violations_and_responses = (
            {'violations': [supervision_violation],
             'violation_responses': [supervision_violation_response]
             }
        )

        expected_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_XX',
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION,
                supervision_violation=supervision_violation
            )

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(12345,
                                  supervision_violations_and_responses)])
                  | 'Set Supervision Violation on '
                    'Supervision Violation Response' >>
                  beam.ParDo(
                      entity_hydration_utils.SetViolationOnViolationsResponse())
                  )

        assert_that(output, equal_to([(12345, expected_violation_response)]))

        test_pipeline.run()

    def testSetViolationOnViolationsResponse_NoViolation(self):
        """Tests that a StateSupervisionViolationResponse is yielded even
        when there is no corresponding violation."""

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_XX',
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION
            )

        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                state_code='US_XX',
                supervision_violation_id=999,
                supervision_violation_responses=[],
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code='US_XX',
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )]
            )

        supervision_violations_and_responses = (
            {'violations': [supervision_violation],
             'violation_responses': [supervision_violation_response]
             }
        )

        expected_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_XX',
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION,
            )

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(12345,
                                  supervision_violations_and_responses)])
                  | 'Set Supervision Violation on '
                    'Supervision Violation Response' >>
                  beam.ParDo(
                      entity_hydration_utils.SetViolationOnViolationsResponse())
                  )

        assert_that(output, equal_to([(12345, expected_violation_response)]))

        test_pipeline.run()


class TestSetSentencesOnSentenceGroup(unittest.TestCase):
    """Tests the SetSentencesOnSentenceGroup DoFn."""
    def testSetSentencesOnSentenceGroup(self):
        """Tests that the hydrated StateIncarcerationSentences and StateSupervisionSentences are set on the
        StateSentenceGroup."""

        incarceration_sentence_id = 123
        supervision_sentence_id = 456

        hydrated_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=incarceration_sentence_id,
            start_date=date(2000, 1, 1),
            charges=[StateCharge.new_with_defaults(
                state_code='US_XX',
                status=ChargeStatus.PRESENT_WITHOUT_INFO,
                ncic_code='1234'
            )],
            incarceration_periods=[
                StateIncarcerationPeriod.new_with_defaults(
                    state_code='US_XX',
                    admission_date=date(2000, 3, 2)
                )
            ]
        )

        hydrated_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_XX',
            supervision_sentence_id=supervision_sentence_id,
            start_date=date(2000, 1, 1),
            charges=[StateCharge.new_with_defaults(
                state_code='US_XX',
                status=ChargeStatus.PRESENT_WITHOUT_INFO,
                ncic_code='1234'
            )],
            incarceration_periods=[
                StateIncarcerationPeriod.new_with_defaults(
                    state_code='US_XX',
                    admission_date=date(2000, 3, 2)
                )
            ]
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[
                StateIncarcerationSentence.new_with_defaults(
                    state_code='US_XX',
                    incarceration_sentence_id=incarceration_sentence_id
                )
            ],
            supervision_sentences=[
                StateSupervisionSentence.new_with_defaults(
                    state_code='US_XX',
                    supervision_sentence_id=supervision_sentence_id
                )
            ]
        )

        person_and_entities = (
            {'incarceration_sentences': [hydrated_incarceration_sentence],
             'supervision_sentences': [hydrated_supervision_sentence],
             'sentence_groups': [sentence_group]
             }
        )

        hydrated_incarceration_sentence_with_group = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=incarceration_sentence_id,
            start_date=date(2000, 1, 1),
            charges=[StateCharge.new_with_defaults(
                state_code='US_XX',
                status=ChargeStatus.PRESENT_WITHOUT_INFO,
                ncic_code='1234'
            )],
            incarceration_periods=[
                StateIncarcerationPeriod.new_with_defaults(
                    state_code='US_XX',
                    admission_date=date(2000, 3, 2)
                )
            ]
        )

        hydrated_supervision_sentence_with_group = StateSupervisionSentence.new_with_defaults(
            state_code='US_XX',
            supervision_sentence_id=supervision_sentence_id,
            start_date=date(2000, 1, 1),
            charges=[StateCharge.new_with_defaults(
                state_code='US_XX',
                status=ChargeStatus.PRESENT_WITHOUT_INFO,
                ncic_code='1234'
            )],
            incarceration_periods=[
                StateIncarcerationPeriod.new_with_defaults(
                    state_code='US_XX',
                    admission_date=date(2000, 3, 2)
                )
            ]
        )

        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[hydrated_incarceration_sentence_with_group],
            supervision_sentences=[hydrated_supervision_sentence_with_group]
        )

        hydrated_incarceration_sentence_with_group.sentence_group = expected_sentence_group
        hydrated_supervision_sentence_with_group.sentence_group = expected_sentence_group

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(12345, person_and_entities)])
                  | 'Set Incarceration and Supervision Sentences on SentenceGroups' >>
                  beam.ParDo(
                      entity_hydration_utils.SetSentencesOnSentenceGroup())
                  )

        assert_that(output, equal_to([(12345, expected_sentence_group)]))

        test_pipeline.run()

    def testSetSentencesOnSentenceGroup_NoSentences(self):
        """Tests that the hydrated StateIncarcerationSentences and StateSupervisionSentences are set on the
        StateSentenceGroup."""
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[],
            supervision_sentences=[]
        )

        person_and_entities = (
            {'incarceration_sentences': [],
             'supervision_sentences': [],
             'sentence_groups': [sentence_group]
             }
        )

        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[],
            supervision_sentences=[]
        )

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(12345, person_and_entities)])
                  | 'Set Incarceration and Supervision Sentences on SentenceGroups' >>
                  beam.ParDo(
                      entity_hydration_utils.SetSentencesOnSentenceGroup())
                  )

        assert_that(output, equal_to([(12345, expected_sentence_group)]))

        test_pipeline.run()
