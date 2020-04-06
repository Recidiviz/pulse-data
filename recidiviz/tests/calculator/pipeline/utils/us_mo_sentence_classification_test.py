# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for classes and helpers in us_mo_sentence_classification.py."""
import datetime
import unittest

from recidiviz.calculator.pipeline.utils.us_mo_sentence_classification import UsMoSentenceStatus, \
    UsMoIncarcerationSentence, UsMoSupervisionSentence
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence, StateSupervisionSentence


class UsMoSentenceStatusTest(unittest.TestCase):
    """Tests for UsMoSentenceStatus"""

    def test_parse_sentence_status(self):
        # Arrange
        raw_dict = {
            'sentence_status_external_id': '1000038-20180619-1-2',
            'status_code': '40I2105',
            'status_date': '20190510',
            'status_description': 'Prob Rev-New Felon-120 Day Trt',
            'sentence_external_id': '1000038-20180619-1'
        }

        # Act
        output = UsMoSentenceStatus.build_from_dictionary(raw_dict)

        # Assert
        self.assertEqual(output,
                         UsMoSentenceStatus(
                             sentence_status_external_id='1000038-20180619-1-2',
                             sentence_external_id='1000038-20180619-1',
                             status_code='40I2105',
                             status_date=datetime.date(year=2019, month=5, day=10),
                             status_description='Prob Rev-New Felon-120 Day Trt'))

        self.assertEqual(output.person_external_id, '1000038')


class MOSentenceTest(unittest.TestCase):
    """Tests for UsMoIncarcerationSentence and UsMoSupervisionSentence."""

    def test_create_mo_supervision_sentence(self):
        # Arrange
        raw_sentence_statuses = [
            {'sentence_external_id': '1345495-20190808-1', 'sentence_status_external_id': '1345495-20190808-1-1',
             'status_code': '15I1000', 'status_date': '20190808', 'status_description': 'New Court Probation'}]

        sentence = StateSupervisionSentence.new_with_defaults(external_id='1345495-20190808-1')

        # Act
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(sentence,
                                                                           raw_sentence_statuses)

        # Assert
        self.assertEqual(us_mo_sentence.base_sentence, sentence)
        self.assertEqual(us_mo_sentence.sentence_statuses,
                         [UsMoSentenceStatus(
                             sentence_status_external_id='1345495-20190808-1-1',
                             sentence_external_id='1345495-20190808-1',
                             status_code='15I1000',
                             status_date=datetime.date(year=2019, month=8, day=8),
                             status_description='New Court Probation')])

        self.assertTrue(isinstance(us_mo_sentence, StateSupervisionSentence))
        self.assertEqual(us_mo_sentence.external_id, sentence.external_id)

    def test_create_mo_incarceration_sentence(self):
        # Arrange
        raw_sentence_statuses = [
            {'sentence_external_id': '1167633-20171012-1', 'sentence_status_external_id': '1167633-20171012-1-1',
             'status_code': '10I1000', 'status_date': '20171012', 'status_description': 'New Court Comm-Institution'},
        ]

        sentence = StateIncarcerationSentence.new_with_defaults(external_id='1167633-20171012-1')

        # Act
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(sentence,
                                                                               raw_sentence_statuses)

        # Assert
        self.assertEqual(us_mo_sentence.base_sentence, sentence)
        self.assertEqual(us_mo_sentence.sentence_statuses,
                         [UsMoSentenceStatus(
                             sentence_status_external_id='1167633-20171012-1-1',
                             sentence_external_id='1167633-20171012-1',
                             status_code='10I1000',
                             status_date=datetime.date(year=2017, month=10, day=12),
                             status_description='New Court Comm-Institution')])

        self.assertTrue(isinstance(us_mo_sentence, StateIncarcerationSentence))
        self.assertEqual(us_mo_sentence.external_id, sentence.external_id)
