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
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence, StateSupervisionSentence


class UsMoSentenceStatusTest(unittest.TestCase):
    """Tests for UsMoSentenceStatus"""

    def _build_test_status(self, status_code: str, status_description: str):
        return UsMoSentenceStatus(
            sentence_status_external_id='1000038-20180619-1-2',
            sentence_external_id='1000038-20180619-1',
            status_code=status_code,
            status_date=datetime.date(year=2019, month=5, day=10),
            status_description=status_description
        )

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
                             status_description='Prob Rev-New Felon-120 Day Trt',
                             is_supervision_in_status=False,
                             is_supervision_out_status=False,
                             is_incarceration_out_status=False,
                             is_sentence_termimination_status=False,
                             is_investigation_status=False,
                             is_lifetime_supervision_start_status=False,
                             is_supervision_type_critical_status=False,
                             supervision_type_critical_status_category=None))

        self.assertEqual(output.person_external_id, '1000038')

    def test_incarceration_supervision_in_out_statuses(self):
        sup_in_status = self._build_test_status(status_code='15I1000', status_description='New Court Probation')
        sup_out_status = self._build_test_status(status_code='45O3010', status_description='CR Ret-Tech Viol')
        inc_in_status = self._build_test_status(status_code='40I0050', status_description='Board Holdover')
        inc_out_status = self._build_test_status(status_code='70O3010',
                                                 status_description='MO Inmate-Interstate Transfer')

        self.assertTrue(sup_in_status.is_supervision_in_status)
        self.assertFalse(sup_out_status.is_supervision_in_status)
        self.assertFalse(inc_in_status.is_supervision_in_status)
        self.assertFalse(inc_out_status.is_supervision_in_status)

        self.assertFalse(sup_in_status.is_supervision_out_status)
        self.assertTrue(sup_out_status.is_supervision_out_status)
        self.assertFalse(inc_in_status.is_supervision_out_status)
        self.assertFalse(inc_out_status.is_supervision_out_status)

        self.assertFalse(sup_in_status.is_incarceration_in_status)
        self.assertFalse(sup_out_status.is_incarceration_in_status)
        self.assertTrue(inc_in_status.is_incarceration_in_status)
        self.assertFalse(inc_out_status.is_incarceration_in_status)

        self.assertFalse(sup_in_status.is_incarceration_out_status)
        self.assertFalse(sup_out_status.is_incarceration_out_status)
        self.assertFalse(inc_in_status.is_incarceration_out_status)
        self.assertTrue(inc_out_status.is_incarceration_out_status)

    def test_is_investigation_status(self):
        investigation_statuses = [
            self._build_test_status(status_code="05I5000", status_description="New Pre-Sentence Investigation"),
            self._build_test_status(status_code="05I5100", status_description="New Community Corr Court Ref"),
            self._build_test_status(status_code="05I5200", status_description="New Interstate Compact-Invest"),
            self._build_test_status(status_code="05I5210", status_description="IS Comp-Reporting Instr Given"),
            self._build_test_status(status_code="05I5300", status_description="New Exec Clemency-Invest"),
            self._build_test_status(status_code="05I5400", status_description="New Bond Investigation"),
            self._build_test_status(status_code="05I5500", status_description="New Diversion Investigation"),
            self._build_test_status(status_code="05I5600", status_description="New Sentencing Assessment"),
            self._build_test_status(status_code="25I5000", status_description="PSI-Addl Charge"),
            self._build_test_status(status_code="25I5100", status_description="Comm Corr Crt Ref-Addl Charge"),
            self._build_test_status(status_code="25I5200", status_description="IS Compact-Invest-Addl Charge"),
            self._build_test_status(status_code="25I5210", status_description="IS Comp-Rep Instr Giv-Addl Chg"),
            self._build_test_status(status_code="25I5300", status_description="Exec Clemency-Invest-Addl Chg"),
            self._build_test_status(status_code="25I5400", status_description="Bond Investigation-Addl Charge"),
            self._build_test_status(status_code="25I5500", status_description="Diversion Invest-Addl Charge"),
            self._build_test_status(status_code="25I5600", status_description="Sentencing Assessment-Addl Chg"),
            self._build_test_status(status_code="35I5000", status_description="PSI-Revisit"),
            self._build_test_status(status_code="35I5100", status_description="Comm Corr Crt Ref-Revisit"),
            self._build_test_status(status_code="35I5200", status_description="IS Compact-Invest-Revisit"),
            self._build_test_status(status_code="35I5210", status_description="IS Comp-Rep Instr Giv-Revisit"),
            self._build_test_status(status_code="35I5400", status_description="Bond Investigation-Revisit"),
            self._build_test_status(status_code="35I5500", status_description="Diversion Invest-Revisit"),
            self._build_test_status(status_code="35I5600", status_description="Sentencing Assessment-Revisit"),
            self._build_test_status(status_code="95O5000", status_description="PSI Completed"),
            self._build_test_status(status_code="95O5010", status_description="PSI Probation Denied-Other"),
            self._build_test_status(status_code="95O5015", status_description="PSI Plea Withdrawn"),
            self._build_test_status(status_code="95O5020", status_description="PSI Probation Denied-Jail"),
            self._build_test_status(status_code="95O5025", status_description="PSI Other Disposition"),
            self._build_test_status(status_code="95O5030", status_description="PSI Cancelled by Court"),
            self._build_test_status(status_code="95O5100", status_description="Comm Corr Court Ref Closed"),
            self._build_test_status(status_code="95O5200", status_description="Interstate Invest Closed"),
            self._build_test_status(status_code="95O5210", status_description="IS Comp-Report Instruct Closed"),
            self._build_test_status(status_code="95O5310", status_description="Executive Clemency Inv Comp."),
            self._build_test_status(status_code="95O5400", status_description="Bond Investigation Closed"),
            self._build_test_status(status_code="95O5405", status_description="Bond Invest-No Charge"),
            self._build_test_status(status_code="95O5500", status_description="Diversion Invest Completed"),
            self._build_test_status(status_code="95O5510", status_description="Diversion Invest Denied"),
            self._build_test_status(status_code="95O5600", status_description="SAR Completed"),
            self._build_test_status(status_code="95O560Z", status_description="SAR Completed      MUST VERIFY"),
            self._build_test_status(status_code="95O5610", status_description="SAR Probation Denied-Other"),
            self._build_test_status(status_code="95O5615", status_description="SAR Plea Withdrawn"),
            self._build_test_status(status_code="95O5620", status_description="SAR Probation Denied-Jail"),
            self._build_test_status(status_code="95O5625", status_description="SAR Other Disposition"),
            self._build_test_status(status_code="95O5630", status_description="SAR Cancelled by Court"),
            self._build_test_status(status_code="95O51ZZ", status_description="Comm Corr Ref Comp MUST VERIFY"),
            self._build_test_status(status_code="95O52ZZ", status_description="Interstate Inv Cls MUST VERIFY")
        ]

        for status in investigation_statuses:
            self.assertTrue(status.is_investigation_status)

        non_investigation_status = self._build_test_status('35I4000', 'IS Compact-Prob-Revisit')
        self.assertFalse(non_investigation_status.is_investigation_status)

    def test_is_sentence_termimination_status(self):
        institutional_sentence_end = \
            self._build_test_status(status_code="90O2010", status_description="Parole Completion")
        supervision_sentence_end = \
            self._build_test_status(status_code="95O1000", status_description="Court Probation Completion")
        death_status = \
            self._build_test_status(status_code="99O9999", status_description="Execution")
        lifetime_supv_status = \
            self._build_test_status(status_code="90O1070", status_description="Director's Rel Comp-Life Supv")
        investigation_completion_status = \
            self._build_test_status(status_code="95O5500", status_description="Diversion Invest Completed")

        self.assertTrue(institutional_sentence_end.is_sentence_termimination_status)
        self.assertTrue(supervision_sentence_end.is_sentence_termimination_status)
        self.assertTrue(death_status.is_sentence_termimination_status)
        self.assertFalse(lifetime_supv_status.is_sentence_termimination_status)
        self.assertFalse(investigation_completion_status.is_sentence_termimination_status)

    def test_is_lifetime_supervision_start_status(self):
        lifetime_supv_status1 = \
            self._build_test_status(status_code="35I6010", status_description="Release from DMH for SVP Supv")
        lifetime_supv_status2 = \
            self._build_test_status(status_code="90O1070", status_description="Director's Rel Comp-Life Supv")

        self.assertTrue(lifetime_supv_status1.is_lifetime_supervision_start_status)
        self.assertTrue(lifetime_supv_status2.is_lifetime_supervision_start_status)


class UsMoSentenceTest(unittest.TestCase):
    """Tests for UsMoIncarcerationSentence and UsMoSupervisionSentence."""

    def setUp(self) -> None:
        self.validation_date = datetime.date(year=2019, month=10, day=31)

    def test_no_statuses_does_not_crash(self):
        raw_statuses = []

        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id='13252-20160627-1',
            start_date=datetime.date(year=2016, month=6, day=27)
        )
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date), None)

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

    def test_supervision_type_new_probation(self):
        raw_statuses = [
            {'sentence_external_id': '1345495-20190808-1', 'sentence_status_external_id': '1345495-20190808-1-1',
             'status_code': '15I1000', 'status_date': '20190808', 'status_description': 'New Court Probation'}
        ]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='1345495-20190808-1',
            start_date=datetime.date(year=2019, month=8, day=8)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_supervision_type_parole(self):
        raw_statuses = [
            {'sentence_external_id': '1167633-20171012-2', 'sentence_status_external_id': '1167633-20171012-2-1',
             'status_code': '10I1000', 'status_date': '20171012', 'status_description': 'New Court Comm-Institution'},
            {'sentence_external_id': '1167633-20171012-2', 'sentence_status_external_id': '1167633-20171012-2-2',
             'status_code': '40O1010', 'status_date': '20190913', 'status_description': 'Parole Release'}]

        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id='1167633-20171012-2',
            start_date=datetime.date(year=2017, month=10, day=12)
        )
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PAROLE)

    def test_supervision_type_court_parole(self):
        # Court Parole is actually probation
        raw_statuses = [
            {"sentence_external_id": "1344959-20190718-1", "sentence_status_external_id": "1344959-20190718-1-1",
             "status_code": "15I1200", "status_date": "20190718", "status_description": "New Court Parole"}
        ]

        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id='1344959-20190718-1',
            start_date=datetime.date(year=2017, month=10, day=12)
        )
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_supervision_type_lifetime_supervision(self):
        raw_statuses = [
            {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-1',
             'status_code': '10I1000', 'status_date': '20160627', 'status_description': 'New Court Comm-Institution'},
            {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-2',
             'status_code': '90O1070', 'status_date': '20190415',
             'status_description': "Director's Rel Comp-Life Supv"},
            {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-3',
             'status_code': '40O6020', 'status_date': '20190415', 'status_description': 'Release for Lifetime Supv'},
        ]

        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id='13252-20160627-1',
            start_date=datetime.date(year=2016, month=6, day=27)
        )
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PAROLE)

    def test_supervision_type_lifetime_supervision_after_inst_completion(self):
        raw_statuses = [
            {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-1',
             'status_code': '10I1000', 'status_date': '20160627', 'status_description': 'New Court Comm-Institution'},
            {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-2',
             'status_code': '90O1010', 'status_date': '20190415',
             'status_description': "Inst. Expiration of Sentence"},
            {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-3',
             'status_code': '40O6020', 'status_date': '20190415', 'status_description': 'Release for Lifetime Supv'},
        ]

        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id='13252-20160627-1',
            start_date=datetime.date(year=2016, month=6, day=27)
        )
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PAROLE)

    def test_lifetime_supervision_no_supervision_in(self):
        raw_statuses = [
            {"sentence_external_id": "1096616-20060515-3", "sentence_status_external_id": "1096616-20060515-3-5",
             "status_code": "20I1000", "status_date": "20090611", "status_description": "Court Comm-Inst-Addl Charge"},
            {"sentence_external_id": "1096616-20060515-3", "sentence_status_external_id": "1096616-20060515-3-10",
             "status_code": "90O1070", "status_date": "20151129", "status_description": "Director's Rel Comp-Life Supv"}
        ]

        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id='1096616-20060515-3',
            start_date=datetime.date(year=2009, month=6, day=11)
        )
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PAROLE)

    def test_probation_after_investigation_status_list_unsorted(self):
        raw_statuses = [
            {'sentence_external_id': '282443-20180427-1', 'sentence_status_external_id': '282443-20180427-1-3',
             'status_code': '35I1000', 'status_date': '20180525', 'status_description': 'Court Probation-Revisit'},
            {'sentence_external_id': '282443-20180427-1', 'sentence_status_external_id': '282443-20180427-1-2',
             'status_code': '95O5630', 'status_date': '20180525', 'status_description': 'SAR Cancelled by Court'},
            {'sentence_external_id': '282443-20180427-1', 'sentence_status_external_id': '282443-20180427-1-1',
             'status_code': '05I5600', 'status_date': '20180427', 'status_description': 'New Sentencing Assessment'}]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='282443-20180427-1',
            start_date=datetime.date(year=2018, month=5, day=25)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_diversion_probation_after_investigation(self):
        raw_statuses = [
            {'sentence_external_id': '1324786-20180214-1', 'sentence_status_external_id': '1324786-20180214-1-1',
             'status_code': '05I5500', 'status_date': '20180214', 'status_description': 'New Diversion Investigation'},
            {'sentence_external_id': '1324786-20180214-1', 'sentence_status_external_id': '1324786-20180214-1-2',
             'status_code': '95O5500', 'status_date': '20180323', 'status_description': 'Diversion Invest Completed'},
            {'sentence_external_id': '1324786-20180214-1', 'sentence_status_external_id': '1324786-20180214-1-3',
             'status_code': '35I2000', 'status_date': '20180323', 'status_description': 'Diversion Supv-Revisit'}]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='1324786-20180214-1',
            start_date=datetime.date(year=2018, month=3, day=23)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

        # Also test that we count the person as on probation if we are looking at the exact day they started this
        # supervision.
        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(datetime.date(year=2018, month=3, day=23)),
                         StateSupervisionType.PROBATION)

    def test_diversion_probation_after_community_court_ref_investigation(self):
        raw_statuses = [
            {'sentence_external_id': '1324786-20180214-1', 'sentence_status_external_id': '1324786-20180214-1-1',
             'status_code': '05I5100', 'status_date': '20180214', 'status_description': 'New Community Corr Court Ref'},
            {'sentence_external_id': '1324786-20180214-1', 'sentence_status_external_id': '1324786-20180214-1-2',
             'status_code': '95O5100', 'status_date': '20180323', 'status_description': 'Comm Corr Court Ref Closed'},
            {'sentence_external_id': '1324786-20180214-1', 'sentence_status_external_id': '1324786-20180214-1-3',
             'status_code': '35I2000', 'status_date': '20180323', 'status_description': 'Diversion Supv-Revisit'}]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='1324786-20180214-1',
            start_date=datetime.date(year=2018, month=3, day=23)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_interstate_compact_parole_classified_as_probation(self):
        raw_statuses = [
            {'sentence_external_id': '165467-20171227-1', 'sentence_status_external_id': '165467-20171227-1-1',
             'status_code': '05I5200', 'status_date': '20171227',
             'status_description': 'New Interstate Compact-Invest'},
            {'sentence_external_id': '165467-20171227-1', 'sentence_status_external_id': '165467-20171227-1-2',
             'status_code': '95O5200', 'status_date': '20180123', 'status_description': 'Interstate Invest Closed'},
            {'sentence_external_id': '165467-20171227-1', 'sentence_status_external_id': '165467-20171227-1-3',
             'status_code': '35I4100', 'status_date': '20180129', 'status_description': 'IS Compact-Parole-Revisit'},
        ]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='165467-20171227-1',
            start_date=datetime.date(year=2018, month=1, day=29)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_probation_starts_same_day_as_new_investigation(self):
        raw_statuses = [
            {'sentence_external_id': '1344336-20190703-1', 'sentence_status_external_id': '1344336-20190703-1-1',
             'status_code': '05I5210', 'status_date': '20190703',
             'status_description': 'IS Comp-Reporting Instr Given'},
            {'sentence_external_id': '1344336-20190703-1', 'sentence_status_external_id': '1344336-20190703-1-2',
             'status_code': '95O5210', 'status_date': '20190716',
             'status_description': 'IS Comp-Report Instruct Closed'},
            {'sentence_external_id': '1344336-20190703-1', 'sentence_status_external_id': '1344336-20190703-1-3',
             'status_code': '35I5200', 'status_date': '20190716', 'status_description': 'IS Compact-Invest-Revisit'},
            {'sentence_external_id': '1344336-20190703-1', 'sentence_status_external_id': '1344336-20190703-1-4',
             'status_code': '95O5200', 'status_date': '20190716', 'status_description': 'Interstate Invest Closed'},
            {'sentence_external_id': '1344336-20190703-1', 'sentence_status_external_id': '1344336-20190703-1-5',
             'status_code': '35I4000', 'status_date': '20190716', 'status_description': 'IS Compact-Prob-Revisit'}]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='1344336-20190703-1',
            start_date=datetime.date(year=2019, month=7, day=16)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_resentenced_probation_revisit(self):
        raw_statuses = [
            {'sentence_external_id': '1254438-20130418-2', 'sentence_status_external_id': '1254438-20130418-2-2',
             'status_code': '25I1000', 'status_date': '20140610', 'status_description': 'Court Probation-Addl Charge'},
            {'sentence_external_id': '1254438-20130418-2', 'sentence_status_external_id': '1254438-20130418-2-8',
             'status_code': '95O1040', 'status_date': '20170717', 'status_description': 'Resentenced'},
            {'sentence_external_id': '1254438-20130418-2', 'sentence_status_external_id': '1254438-20130418-2-9',
             'status_code': '35I1000', 'status_date': '20170717', 'status_description': 'Court Probation-Revisit'}
        ]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='1254438-20130418-2',
            start_date=datetime.date(year=2014, month=6, day=10)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_release_to_field_other_sentence(self):
        raw_statuses = [
            {'sentence_external_id': '1328840-20180523-3', 'sentence_status_external_id': '1328840-20180523-3-2',
             'status_code': '25I1000', 'status_date': '20180523', 'status_description': 'Court Probation-Addl Charge'},
            {'sentence_external_id': '1328840-20180523-3', 'sentence_status_external_id': '1328840-20180523-3-3',
             'status_code': '40I7000', 'status_date': '20181011',
             'status_description': 'Field Supv to DAI-Oth Sentence'},
            {'sentence_external_id': '1328840-20180523-3', 'sentence_status_external_id': '1328840-20180523-3-4',
             'status_code': '45O7000', 'status_date': '20181011', 'status_description': 'Field to DAI-Other Sentence'},
            {'sentence_external_id': '1328840-20180523-3', 'sentence_status_external_id': '1328840-20180523-3-5',
             'status_code': '40O7000', 'status_date': '20181017', 'status_description': 'Rel to Field-DAI Other Sent'}
        ]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='1328840-20180523-3',
            start_date=datetime.date(year=2014, month=6, day=10)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_prob_rev_codes_not_applicable(self):
        raw_statuses = [
            {'sentence_external_id': '1163420-20180116-1', 'sentence_status_external_id': '1163420-20180116-1-1',
             'status_code': '15I1000', 'status_date': '20180116', 'status_description': 'New Court Probation'},
            {'sentence_external_id': '1163420-20180116-1', 'sentence_status_external_id': '1163420-20180116-1-3',
             'status_code': '95O2120', 'status_date': '20180925',
             'status_description': 'Prob Rev-Codes Not Applicable'},
            {'sentence_external_id': '1163420-20180116-1', 'sentence_status_external_id': '1163420-20180116-1-4',
             'status_code': '35I1000', 'status_date': '20180925', 'status_description': 'Court Probation-Revisit'}]

        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id='1163420-20180116-1',
            start_date=datetime.date(year=2014, month=6, day=10)
        )
        us_mo_sentence = UsMoSupervisionSentence.from_supervision_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date),
                         StateSupervisionType.PROBATION)

    def test_incarcerated_on_date(self):
        raw_statuses = [
            {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-1',
             'status_code': '10I1000', 'status_date': '20160627', 'status_description': 'New Court Comm-Institution'}
        ]

        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id='13252-20160627-1',
            start_date=datetime.date(year=2016, month=6, day=27)
        )
        us_mo_sentence = UsMoIncarcerationSentence.from_incarceration_sentence(base_sentence, raw_statuses)

        self.assertEqual(us_mo_sentence.get_sentence_supervision_type_on_day(self.validation_date), None)
