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
"""Tests for the functions in us_id_supervision_compliance.py"""
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import \
    us_id_case_compliance_on_date, NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS, \
    SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION, \
    SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE, SEX_OFFENDER_LSIR_MINIMUM_SCORE, \
    _assessment_is_up_to_date, \
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS, _face_to_face_contact_frequency_is_sufficient, \
    _guidelines_applicable_for_case, _assessments_in_compliance_month, _face_to_face_contacts_in_compliance_month, \
    _is_new_level_system, MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE, \
    MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE, \
    HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE, \
    DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE,\
    DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE, \
    DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE, \
    MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE, \
    HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import StateSupervisionContactType, \
    StateSupervisionContactStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodTerminationReason, \
    StateSupervisionPeriodSupervisionType, StateSupervisionPeriodAdmissionReason, StateSupervisionLevel
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateAssessment, StateSupervisionContact

class TestCaseCompliance(unittest.TestCase):
    """Tests the us_id_case_compliance_on_date function."""

    def test_us_id_case_compliance_on_date(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MODERATE'
        )

        case_type = StateSupervisionCaseType.GENERAL

        assessments = [StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )]

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=date(2018, 4, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        compliance = us_id_case_compliance_on_date(
            supervision_period,
            case_type,
            start_of_supervision,
            compliance_evaluation_date,
            assessments,
            supervision_contacts
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                assessment_up_to_date=True,
                face_to_face_count=1,
                face_to_face_frequency_sufficient=True
            ), compliance)

    def test_us_id_case_compliance_on_date_no_assessment_no_contacts(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM
            )

        case_type = StateSupervisionCaseType.GENERAL

        start_of_supervision = date(2018, 1, 5)
        compliance_evaluation_date = date(2018, 4, 30)

        compliance = us_id_case_compliance_on_date(
            supervision_period,
            case_type,
            start_of_supervision,
            compliance_evaluation_date,
            assessments=[],
            supervision_contacts=[]
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                assessment_up_to_date=False,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False
            ), compliance)

    def test_us_id_case_compliance_on_date_not_applicable_case(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=None  # Must have a supervision level to be evaluated
            )

        assessments = [StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )]

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=date(2018, 3, 6),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        case_type = StateSupervisionCaseType.GENERAL

        start_of_supervision = date(2018, 1, 5)
        compliance_evaluation_date = date(2018, 3, 31)

        compliance = us_id_case_compliance_on_date(
            supervision_period,
            case_type,
            start_of_supervision,
            compliance_evaluation_date,
            assessments,
            supervision_contacts
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=1,
                assessment_up_to_date=None,
                face_to_face_count=1,
                face_to_face_frequency_sufficient=None
            ), compliance)


class TestAssessmentsInComplianceMonth(unittest.TestCase):
    """Tests for _assessments_in_compliance_month."""
    def test_assessments_in_compliance_month(self):
        evaluation_date = date(2018, 4, 30)
        assessment_out_of_range = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 3, 10)
        )
        assessment_out_of_range_2 = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 5, 10)
        )
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 2)
        )
        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 10)
        )
        assessment_3 = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28)
        )
        assessments = [assessment_out_of_range, assessment_out_of_range_2, assessment_1, assessment_2, assessment_3]
        expected_assessments = [assessment_1, assessment_2, assessment_3]

        self.assertEqual(len(expected_assessments), _assessments_in_compliance_month(evaluation_date, assessments))


class TestFaceToFaceContactsInComplianceMonth(unittest.TestCase):
    """Tests for _face_to_face_contacts_in_compliance_month."""
    def test_face_to_face_contacts_in_compliance_month(self):
        evaluation_date = date(2018, 4, 30)
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=date(2018, 4, 1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=date(2018, 4, 15),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_3 = StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_out_of_range = StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=date(2018, 3, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_incomplete = StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )
        contact_wrong_type = StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contacts = [contact_1, contact_2, contact_3, contact_incomplete, contact_out_of_range, contact_wrong_type]
        expected_contacts = [contact_1, contact_2, contact_3]
        self.assertEqual(len(expected_contacts), _face_to_face_contacts_in_compliance_month(evaluation_date, contacts))


class TestAssessmentIsUpToDate(unittest.TestCase):
    """Tests the _assessment_is_up_to_date function."""

    def test_assessment_is_up_to_date(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_no_assessment_new_period(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_no_assessment_old_period(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_assessment_is_up_to_date_assessment_before_starting_parole(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3)
        )

        # This person started on parole more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_assessment_before_starting_dual(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3)
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_assessment_before_starting_probation(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3)
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_old_assessment_minimum_level_deprecated(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text='LEVEL 1'
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3)
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_old_assessment_minimum_level(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text='LOW'
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3)
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_old_assessment_not_minimum_level(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3)
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertFalse(assessment_up_to_date)

    def test_assessment_is_up_to_date_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_no_assessment_new_period_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_no_assessment_old_period_probation_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        # This person started on probation more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance.
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.GENERAL,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_assessment_is_up_to_date_no_assessment_old_period_parole_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
            )

        # This person started on parole more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE ago,
        # and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_assessment_is_up_to_date_assessment_before_starting_parole_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 10, 3)
        )

        # This person started on parole more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE ago,
        # and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_assessment_before_starting_dual_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 10, 3)
        )

        # This person started on probation more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_assessment_before_starting_probation_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3)
        )

        # This person started on probation more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_old_assessment_greater_than_minimum_lsir_score_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=SEX_OFFENDER_LSIR_MINIMUM_SCORE + 1,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3)
        )

        # This person started on probation more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
            relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertFalse(assessment_up_to_date)

    def test_assessment_is_up_to_date_old_assessment_less_than_minimum_lsir_score_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=SEX_OFFENDER_LSIR_MINIMUM_SCORE - 1,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3)
        )

        # This person started on probation more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_assessment_is_up_to_date_no_old_assessment_sex_offender(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        # This person started on probation more than SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            StateSupervisionCaseType.SEX_OFFENDER,
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)


class TestContactFrequencySufficient(unittest.TestCase):
    """Tests the _contact_frequency_is_sufficient function."""
    def test_face_to_face_frequency_sufficient_start_of_supervision_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS - 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_before_supervision_start_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            # Only contact happened before supervision started
            contact_date=supervision_period.start_date - \
            relativedelta(days=100),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_attempted_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            # Only contact was not completed
            status=StateSupervisionContactStatus.ATTEMPTED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_invalid_contact_type_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            # Only contact is invalid type
            contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text='LOW'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date + relativedelta(days=1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_deprecated_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text='LEVEL 1'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date + relativedelta(days=1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision \
                                     + relativedelta(days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maximum_level_up_to_date_one_contact_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text='LEVEL 4'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date + relativedelta(days=1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maximum_level_up_to_date_two_contacts_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text='LEVEL 4'
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(days=30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(days=40),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            )
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision + relativedelta(days=50)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='HIGH'
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date +
                relativedelta(days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date +
                relativedelta(days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 5),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            )
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision + \
            relativedelta(days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_deprecated_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='LEVEL 3'
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 5),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            )
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision + \
                                     relativedelta(days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maximum_level_not_up_to_date_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text='LEVEL 4'
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date
                + relativedelta(days=DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            )
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 10)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_up_to_date_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='HIGH'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision \
            + relativedelta(days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_up_to_date_deprecated_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='LEVEL 3'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision + relativedelta(days=(
            DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_not_up_to_date_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='HIGH'
        )

        # One contact within the time window, one outside.
        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(days=15),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            )
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision \
            + relativedelta(days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_not_up_to_date_deprecated_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='LEVEL 3'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision + relativedelta(
            days=(DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_up_to_date_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MODERATE'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision \
            + relativedelta(days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_up_to_date_deprecated_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='LEVEL 2'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision + relativedelta(
            days=(DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_not_up_to_date_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MODERATE'
        )

        # One contact within the time window, one outside.
        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(days=15),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            )
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision \
            + relativedelta(days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_not_up_to_date_deprecated_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='LEVEL 2'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision + relativedelta(
            days=(DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 10))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_new_case_opened_on_friday_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(1999, 8, 13),  # This was a Friday
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MODERATE'
        )

        supervision_contacts = []

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision \
            + relativedelta(days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_new_case_opened_on_friday_deprecated_general_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(1999, 8, 13),  # This was a Friday
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='LEVEL 2'
        )

        supervision_contacts = []

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision \
                                     + relativedelta(days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.GENERAL, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_sex_offender_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text='SO LEVEL 1'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date + relativedelta(days=1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE - 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.SEX_OFFENDER, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_sex_offender_case_not_met(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text='SO LEVEL 1'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date + relativedelta(days=1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE + 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.SEX_OFFENDER, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_sex_offender_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='SO LEVEL 2'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date + relativedelta(days=1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE - 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.SEX_OFFENDER, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_sex_offender_case_not_met(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='SO LEVEL 2'
        )

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            contact_date=supervision_period.start_date + relativedelta(days=1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE + 1))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.SEX_OFFENDER, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_sex_offender_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='SO LEVEL 3'
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE - 10),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE - 20),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            )
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.SEX_OFFENDER, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_sex_offender_case_not_met(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='SO LEVEL 3'
        )

        # One contact within in time period, and one after.
        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE - 10),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code='US_ID',
                contact_date=supervision_period.start_date + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE + 10),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = start_of_supervision\
            + relativedelta(days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE))

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(
            StateSupervisionCaseType.SEX_OFFENDER, supervision_period, start_of_supervision, compliance_evaluation_date,
            supervision_contacts)

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_is_new_level_system_case_sensitivity(self):
        self.assertTrue(_is_new_level_system("LOW"))
        self.assertFalse(_is_new_level_system("Low"))


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the _guidelines_applicable_for_case function."""

    def test_guidelines_applicable_for_case_general(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='LEVEL 2'
        )

        case_type = StateSupervisionCaseType.GENERAL

        applicable = _guidelines_applicable_for_case(
            supervision_period,
            case_type
        )

        self.assertTrue(applicable)

    def test_guidelines_applicable_for_case_no_supervision_level_general(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=None
        )

        case_type = StateSupervisionCaseType.GENERAL

        applicable = _guidelines_applicable_for_case(
            supervision_period,
            case_type
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_type_general(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='LEVEL 3'
        )

        case_type = StateSupervisionCaseType.GENERAL

        applicable = _guidelines_applicable_for_case(
            supervision_period,
            case_type
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_case_type(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='LEVEL 3'
        )

        case_type = StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS

        applicable = _guidelines_applicable_for_case(
            supervision_period,
            case_type
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_sex_offender(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='SO LEVEL 2'
        )

        case_type = StateSupervisionCaseType.SEX_OFFENDER

        applicable = _guidelines_applicable_for_case(
            supervision_period,
            case_type
        )

        self.assertTrue(applicable)

    def test_guidelines_not_applicable_for_case_invalid_leveL_sex_offender(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text='SO LEVEL 4'
        )

        case_type = StateSupervisionCaseType.SEX_OFFENDER

        applicable = _guidelines_applicable_for_case(
            supervision_period,
            case_type
        )

        self.assertFalse(applicable)

    def test_guidelines_not_applicable_for_case_invalid_supervision_type_sex_offender(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text='SO LEVEL 4'
        )

        case_type = StateSupervisionCaseType.SEX_OFFENDER

        applicable = _guidelines_applicable_for_case(
            supervision_period,
            case_type
        )

        self.assertFalse(applicable)
