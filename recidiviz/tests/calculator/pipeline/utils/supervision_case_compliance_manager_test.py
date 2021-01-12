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
"""Unit tests for supervision_case_compliance_manager."""
# pylint: disable=protected-access
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import \
    UsNdSupervisionCaseCompliance
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import \
    NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS, \
    SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION, \
    SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE, SEX_OFFENSE_LSIR_MINIMUM_SCORE, \
    UsIdSupervisionCaseCompliance
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import \
    NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import StateSupervisionContactType, \
    StateSupervisionContactStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodTerminationReason, \
    StateSupervisionPeriodSupervisionType, StateSupervisionPeriodAdmissionReason, StateSupervisionLevel
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateAssessment, StateSupervisionContact


class TestCaseCompliance(unittest.TestCase):
    """Tests the get_case_compliance_on_date function."""

    def test_us_nd_get_case_compliance_on_date_no_assessments_but_within_six_months(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MODERATE'
        )

        case_type = StateSupervisionCaseType.GENERAL

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        assessments = []
        face_to_face_contacts = []

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(supervision_period,
                                                                     case_type,
                                                                     start_of_supervision,
                                                                     assessments,
                                                                     face_to_face_contacts)
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(compliance_evaluation_date)

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                assessment_up_to_date=True,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False
            ), compliance)

    def test_us_nd_get_case_compliance_on_date_with_assessments_within_six_months(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2020, 12, 31),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MODERATE'
        )

        case_type = StateSupervisionCaseType.GENERAL

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2020, 4, 30)

        assessments = [StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2020, 4, 10)
        )]

        face_to_face_contacts = []

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(supervision_period,
                                                                     case_type,
                                                                     start_of_supervision,
                                                                     assessments,
                                                                     face_to_face_contacts)
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(compliance_evaluation_date)

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=1,
                assessment_up_to_date=True,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False
            ), compliance)

    def test_us_nd_get_case_compliance_on_date_with_assessments_outside_six_months(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 2, 5),
            termination_date=date(2020, 12, 31),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MODERATE'
        )

        case_type = StateSupervisionCaseType.GENERAL

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2020, 2, 27)

        assessments = [StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2020, 12, 10)
        )]

        face_to_face_contacts = []

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(supervision_period,
                                                                     case_type,
                                                                     start_of_supervision,
                                                                     assessments,
                                                                     face_to_face_contacts)
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(compliance_evaluation_date)

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                assessment_up_to_date=False,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False
            ), compliance)

    def test_us_id_get_case_compliance_on_date(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code=StateCode.US_ID.value,
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
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )]

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     case_type,
                                                                     start_of_supervision,
                                                                     assessments,
                                                                     supervision_contacts)
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(compliance_evaluation_date)

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                assessment_up_to_date=True,
                face_to_face_count=1,
                face_to_face_frequency_sufficient=True
            ), compliance)

    def test_us_id_get_case_compliance_on_date_with_virtual_contact_on_date(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            state_code=StateCode.US_ID.value,
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
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )]

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.VIRTUAL,
                status=StateSupervisionContactStatus.COMPLETED
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED
            ),
        ]

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     case_type,
                                                                     start_of_supervision,
                                                                     assessments,
                                                                     supervision_contacts)
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(compliance_evaluation_date)

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                assessment_up_to_date=True,
                face_to_face_count=1,
                face_to_face_frequency_sufficient=True
            ), compliance)

    def test_us_id_get_case_compliance_on_date_no_assessment_no_contacts(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     case_type,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(compliance_evaluation_date)
        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                assessment_up_to_date=False,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False
            ), compliance)

    def test_us_id_get_case_compliance_on_date_not_applicable_case(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=None  # Must have a supervision level to be evaluated
            )

        assessments = [StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )]

        supervision_contacts = [StateSupervisionContact.new_with_defaults(
            state_code=StateCode.US_ID.value,
            contact_date=date(2018, 3, 6),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED
        )]

        case_type = StateSupervisionCaseType.GENERAL

        start_of_supervision = date(2018, 1, 5)
        compliance_evaluation_date = date(2018, 3, 31)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     case_type,
                                                                     start_of_supervision,
                                                                     assessments,
                                                                     supervision_contacts)
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(compliance_evaluation_date)

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=1,
                assessment_up_to_date=False,
                face_to_face_count=1,
                face_to_face_frequency_sufficient=False
            ), compliance)


class TestAssessmentIsUpToDate(unittest.TestCase):
    """Tests the _assessment_is_up_to_date function."""

    def test_us_id_assessment_is_up_to_date(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_no_assessment_new_period(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_no_assessment_old_period(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_assessment_before_starting_parole(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_assessment_before_starting_dual(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )
        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_assessment_before_starting_probation(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_old_assessment_minimum_level_deprecated(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text='LEVEL 1'
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_old_assessment_minimum_level(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text='LOW'
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_old_assessment_not_minimum_level(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.GENERAL,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_no_assessment_new_period_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_no_assessment_old_period_probation_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance.
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_no_assessment_old_period_parole_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
            )

        # This person started on parole more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE ago,
        # and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_assessment_before_starting_parole_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 10, 3)
        )

        # This person started on parole more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE ago,
        # and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_assessment_before_starting_dual_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 10, 3)
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_assessment_before_starting_probation_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3)
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_old_assessment_greater_than_minimum_lsir_score_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=SEX_OFFENSE_LSIR_MINIMUM_SCORE + 1,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3)
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_old_assessment_less_than_minimum_lsir_score_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=SEX_OFFENSE_LSIR_MINIMUM_SCORE - 1,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3)
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_id_assessment_is_up_to_date_no_old_assessment_sex_offense(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ID.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])

        assessment_up_to_date = us_id_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_nd_assessment_is_up_to_date_no_assessment_initial_number_of_days(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ND.value,
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        # This person started on probation more than NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [],
                                                                     [])

        assessment_up_to_date = us_nd_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            most_recent_assessment=None
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_nd_assessment_is_up_to_date_with_assessment_before_initial_number_of_days(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ND.value,
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
        )

        # This person started on probation more than NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE)
        compliance_evaluation_date = supervision_period.start_date - \
                                     relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_nd_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertTrue(assessment_up_to_date)

    def test_us_nd_assessment_is_up_to_date_with_assessment_after_initial_number_of_days_no_date(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ND.value,
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
        )

        # This person started on probation more than NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_nd_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertFalse(assessment_up_to_date)

    def test_us_nd_assessment_is_up_to_date_with_assessment_after_initial_number_of_days_with_date(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code=StateCode.US_ND.value,
                start_date=date(2018, 3, 5),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2010, 2, 2)
        )

        # This person started on probation more than NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
                               relativedelta(days=NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE)
        compliance_evaluation_date = supervision_period.start_date + \
                                     relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(supervision_period,
                                                                     StateSupervisionCaseType.SEX_OFFENSE,
                                                                     start_of_supervision,
                                                                     [assessment],
                                                                     [])

        assessment_up_to_date = us_nd_supervision_compliance._assessment_is_up_to_date(
            compliance_evaluation_date,
            assessment
        )

        self.assertFalse(assessment_up_to_date)
