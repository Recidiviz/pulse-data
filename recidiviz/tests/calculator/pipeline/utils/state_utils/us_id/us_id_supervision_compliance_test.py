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
    us_id_case_compliance_on_date, NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS, _assessment_is_up_to_date
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodTerminationReason, \
    StateSupervisionPeriodSupervisionType, StateSupervisionPeriodAdmissionReason, StateSupervisionLevel
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateAssessment


class TestCaseCompliance(unittest.TestCase):
    """Tests the us_id_case_compliance_on_date function."""
    def test_us_id_case_compliance_on_date(self):
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

        compliance = us_id_case_compliance_on_date(
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            assessment
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_up_to_date=True
            ), compliance)

    def test_us_id_case_compliance_on_date_assessment_not_up_to_date(self):
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
            assessment_date=date(2000, 3, 10)
        )

        start_of_supervision = supervision_period.start_date
        compliance_evaluation_date = date(2018, 4, 30)

        compliance = us_id_case_compliance_on_date(
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            assessment
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_up_to_date=False
            ), compliance)


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
        compliance_evaluation_date = supervision_period.start_date + relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
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
        compliance_evaluation_date = supervision_period.start_date + relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
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

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = supervision_period.start_date - \
            relativedelta(days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS)
        compliance_evaluation_date = supervision_period.start_date + relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertFalse(assessment_up_to_date)

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
        compliance_evaluation_date = supervision_period.start_date + relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertFalse(assessment_up_to_date)

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
        compliance_evaluation_date = supervision_period.start_date + relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
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
                supervision_level=StateSupervisionLevel.MINIMUM
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
        compliance_evaluation_date = supervision_period.start_date + relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
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
        compliance_evaluation_date = supervision_period.start_date + relativedelta(days=1)

        assessment_up_to_date = _assessment_is_up_to_date(
            supervision_period,
            start_of_supervision,
            compliance_evaluation_date,
            most_recent_assessment=assessment
        )

        self.assertFalse(assessment_up_to_date)
