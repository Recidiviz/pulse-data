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
from mock import patch, MagicMock

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    UsNdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS,
    SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION,
    SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE,
    SEX_OFFENSE_LSIR_MINIMUM_SCORE,
    UsIdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE,
)
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentType,
    StateAssessmentLevel,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactType,
    StateSupervisionContactStatus,
    StateSupervisionContactLocation,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionLevel,
    StateSupervisionPeriodStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateAssessment,
    StateSupervisionContact,
)


class TestCaseCompliance(unittest.TestCase):
    """Tests the get_case_compliance_on_date function."""

    @patch.object(UsNdSupervisionCaseCompliance, "_guidelines_applicable_for_case")
    def test_us_nd_guidelines_not_applicable_provided(
        self, guidelines_fn: MagicMock
    ) -> None:
        guidelines_fn.return_value = False

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL
        compliance_evaluation_date = date(2018, 4, 30)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=[],
        )
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        assert compliance is not None
        self.assertIsNone(compliance.num_days_assessment_overdue)
        self.assertIsNone(compliance.face_to_face_frequency_sufficient)

    @patch.object(UsIdSupervisionCaseCompliance, "_guidelines_applicable_for_case")
    def test_us_id_guidelines_not_applicable_provided(
        self, guidelines_fn: MagicMock
    ) -> None:
        guidelines_fn.return_value = False

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL
        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=[],
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        assert compliance is not None
        self.assertIsNone(compliance.num_days_assessment_overdue)
        self.assertIsNone(compliance.face_to_face_frequency_sufficient)

    def test_us_nd_get_case_compliance_on_date_no_assessments_but_within_one_month(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL
        compliance_evaluation_date = date(2018, 3, 31)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=[],
        )
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                num_days_assessment_overdue=0,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False,
                home_visit_count=0,
                home_visit_frequency_sufficient=True,
            ),
            compliance,
        )

    def test_us_nd_get_case_compliance_on_date_with_assessments_within_one_month(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2020, 12, 31),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL
        compliance_evaluation_date = date(2020, 3, 31)

        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ND.value,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=33,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_date=date(2020, 3, 6),
            )
        ]

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=assessments,
            supervision_contacts=[],
        )
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                most_recent_assessment_date=date(2020, 3, 6),
                num_days_assessment_overdue=0,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False,
                home_visit_count=0,
                home_visit_frequency_sufficient=False,
            ),
            compliance,
        )

    def test_us_nd_get_case_compliance_on_date_with_assessments_outside_six_months(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 2, 5),
            termination_date=date(2020, 12, 31),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL
        compliance_evaluation_date = date(2020, 2, 27)

        # The assessment is outside the 30 day initial compliance window.
        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ND.value,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=33,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_date=date(2020, 12, 10),
            )
        ]

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 2, 5),
            assessments=assessments,
            supervision_contacts=[],
        )
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                num_days_assessment_overdue=722,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False,
                home_visit_frequency_sufficient=False,
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_nd_get_case_compliance_on_date_with_assessments_outside_one_month_within_six(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 2, 5),
            termination_date=date(2020, 12, 31),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL
        compliance_evaluation_date = date(2018, 7, 31)

        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ND.value,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=33,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_date=date(2018, 7, 10),
            )
        ]

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 2, 5),
            assessments=assessments,
            supervision_contacts=[],
        )
        compliance = us_nd_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                most_recent_assessment_date=date(2018, 7, 10),
                assessment_count=0,
                num_days_assessment_overdue=0,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False,
                home_visit_count=0,
                home_visit_frequency_sufficient=False,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ID.value,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=33,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_date=date(2018, 3, 10),
            )
        ]

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        compliance_evaluation_date = date(2018, 4, 30)
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=assessments,
            supervision_contacts=supervision_contacts,
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                most_recent_assessment_date=date(2018, 3, 10),
                num_days_assessment_overdue=0,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2018, 4, 30),
                face_to_face_frequency_sufficient=True,
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_with_virtual_contact_on_date(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ID.value,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=33,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_date=date(2018, 3, 10),
            )
        ]

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.VIRTUAL,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        compliance_evaluation_date = date(2018, 4, 6)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=assessments,
            supervision_contacts=supervision_contacts,
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                most_recent_assessment_date=date(2018, 3, 10),
                num_days_assessment_overdue=0,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2018, 4, 6),
                face_to_face_frequency_sufficient=True,
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_no_assessment_no_contacts(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        start_of_supervision = date(2018, 1, 5)
        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )
        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                num_days_assessment_overdue=70,
                face_to_face_count=0,
                face_to_face_frequency_sufficient=False,
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_not_applicable_case(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=None,  # Must have a supervision level to be evaluated
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ID.value,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=33,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_date=date(2018, 3, 31),
            )
        ]

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 31),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        case_type = StateSupervisionCaseType.DRUG_COURT

        start_of_supervision = date(2018, 1, 5)
        compliance_evaluation_date = date(2018, 3, 31)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_of_supervision,
            assessments=assessments,
            supervision_contacts=supervision_contacts,
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=1,
                num_days_assessment_overdue=None,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2018, 3, 31),
                face_to_face_frequency_sufficient=None,
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_no_home_visits_no_contacts(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        compliance_evaluation_date = date(2018, 4, 30)
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=[],
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=0,
                most_recent_face_to_face_date=None,
                face_to_face_frequency_sufficient=False,
                most_recent_home_visit_date=None,
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_no_home_visits_some_contacts(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.SUPERVISION_OFFICE,
            ),
        ]

        compliance_evaluation_date = date(2018, 4, 30)
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=supervision_contacts,
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2018, 4, 30),
                face_to_face_frequency_sufficient=True,
                most_recent_home_visit_date=None,
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_some_home_visits(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
        ]

        compliance_evaluation_date = date(2018, 4, 30)
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=supervision_contacts,
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2018, 4, 30),
                face_to_face_frequency_sufficient=True,
                most_recent_home_visit_date=date(2018, 4, 30),
                home_visit_count=1,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_one_home_visits_other_ftf(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT,
            ),
        ]

        compliance_evaluation_date = date(2018, 4, 30)
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=supervision_contacts,
        )
        compliance = us_id_supervision_compliance.get_case_compliance_on_date(
            compliance_evaluation_date
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2018, 4, 30),
                face_to_face_frequency_sufficient=True,
                most_recent_home_visit_date=date(2018, 3, 6),
                home_visit_count=0,
            ),
            compliance,
        )

    def test_us_id_get_case_compliance_on_date_multiple_visits_multiple_periods_home_visits(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        supervision_contacts_1 = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
        ]

        supervision_contacts_2 = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2019, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2019, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
        ]

        compliance_evaluation_date_1 = date(2018, 4, 30)
        compliance_evaluation_date_2 = date(2019, 4, 30)

        us_id_supervision_compliance_1 = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period_1,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=supervision_contacts_1,
        )
        compliance_1 = us_id_supervision_compliance_1.get_case_compliance_on_date(
            compliance_evaluation_date_1
        )

        us_id_supervision_compliance_2 = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period_2,
            case_type=case_type,
            start_of_supervision=date(2019, 3, 5),
            assessments=[],
            supervision_contacts=supervision_contacts_2,
        )
        compliance_2 = us_id_supervision_compliance_2.get_case_compliance_on_date(
            compliance_evaluation_date_2
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date_1,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2018, 4, 30),
                face_to_face_frequency_sufficient=True,
                most_recent_home_visit_date=date(2018, 4, 30),
                home_visit_count=1,
            ),
            compliance_1,
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date_2,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=1,
                most_recent_face_to_face_date=date(2019, 4, 30),
                face_to_face_frequency_sufficient=True,
                most_recent_home_visit_date=date(2019, 4, 30),
                home_visit_count=1,
            ),
            compliance_2,
        )

    def test_us_id_get_case_compliance_on_date_home_visit_in_diff_period(self) -> None:
        """Tests when there are two periods and a home visit assigned to each period,
        but each home visit date is outside the bounds of each supervision periods dates
        causing the most_recent_home_visit to be None"""
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.GENERAL

        supervision_contacts_1 = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2019, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2019, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
        ]

        supervision_contacts_2 = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 3, 6),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ID.value,
                contact_date=date(2018, 4, 30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
        ]

        compliance_evaluation_date_1 = date(2018, 4, 30)
        compliance_evaluation_date_2 = date(2019, 4, 30)

        us_id_supervision_compliance_1 = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period_1,
            case_type=case_type,
            start_of_supervision=date(2018, 3, 5),
            assessments=[],
            supervision_contacts=supervision_contacts_1,
        )
        compliance_1 = us_id_supervision_compliance_1.get_case_compliance_on_date(
            compliance_evaluation_date_1
        )

        us_id_supervision_compliance_2 = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period_2,
            case_type=case_type,
            start_of_supervision=date(2019, 3, 5),
            assessments=[],
            supervision_contacts=supervision_contacts_2,
        )
        compliance_2 = us_id_supervision_compliance_2.get_case_compliance_on_date(
            compliance_evaluation_date_2
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date_1,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=0,
                most_recent_face_to_face_date=None,
                face_to_face_frequency_sufficient=False,
                most_recent_home_visit_date=None,
                home_visit_count=0,
            ),
            compliance_1,
        )

        self.assertEqual(
            SupervisionCaseCompliance(
                date_of_evaluation=compliance_evaluation_date_2,
                assessment_count=0,
                most_recent_assessment_date=None,
                num_days_assessment_overdue=11,
                face_to_face_count=0,
                most_recent_face_to_face_date=None,
                face_to_face_frequency_sufficient=False,
                most_recent_home_visit_date=None,
                home_visit_count=0,
            ),
            compliance_2,
        )


class TestNumDaysAssessmentOverdue(unittest.TestCase):
    """Tests the _num_days_assessment_overdue function."""

    def test_us_id_num_days_assessment_overdue(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10),
        )

        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=date(2018, 3, 5),
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_no_assessment_new_period(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = start_date
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, most_recent_assessment=None
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_no_assessment_old_period(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, most_recent_assessment=None
        )

        self.assertEqual(days_overdue, 1)

    def test_us_id_num_days_assessment_overdue_assessment_before_starting_parole(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3),
        )

        # This person started on parole more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_assessment_before_starting_dual(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3),
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )
        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_assessment_before_starting_probation(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3),
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_old_assessment_minimum_level_deprecated(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LEVEL 1",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3),
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_old_assessment_minimum_level(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3),
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_old_assessment_not_minimum_level(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3),
        )

        # This person started on probation more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS ago, and they do not have
        # a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 62)

    def test_us_id_num_days_assessment_overdue_sex_offense(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10),
        )

        start_of_supervision = start_date
        compliance_evaluation_date = date(2018, 4, 30)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_no_assessment_new_period_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = start_date
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, most_recent_assessment=None
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_no_assessment_old_period_probation_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance.
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, most_recent_assessment=None
        )

        self.assertEqual(days_overdue, 1)

    def test_us_id_num_days_assessment_overdue_no_assessment_old_period_parole_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # This person started on parole more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE ago,
        # and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, most_recent_assessment=None
        )

        self.assertEqual(days_overdue, 1)

    def test_us_id_num_days_assessment_overdue_assessment_before_starting_parole_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 10, 3),
        )

        # This person started on parole more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE ago,
        # and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_assessment_before_starting_dual_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 10, 3),
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_assessment_before_starting_probation_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 1, 3),
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_old_assessment_greater_than_minimum_lsir_score_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=SEX_OFFENSE_LSIR_MINIMUM_SCORE[Gender.FEMALE] + 1,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3),
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 62)

    def test_us_id_num_days_assessment_overdue_old_assessment_less_than_minimum_lsir_score_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=SEX_OFFENSE_LSIR_MINIMUM_SCORE[Gender.FEMALE] - 1,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2017, 1, 3),
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_id_num_days_assessment_overdue_no_old_assessment_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # This person started on probation more than SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        days_overdue = us_id_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, most_recent_assessment=None
        )

        self.assertEqual(days_overdue, 1)

    def test_us_nd_num_days_assessment_overdue_no_assessment_initial_number_of_days(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # This person started on probation more than LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        days_overdue = us_nd_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, most_recent_assessment=None
        )

        self.assertEqual(days_overdue, 1)

    def test_us_nd_num_days_assessment_overdue_with_assessment_before_initial_number_of_days(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
        )

        # This person started on probation more than LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        )
        compliance_evaluation_date = start_date - relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_nd_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 0)

    def test_us_nd_num_days_assessment_overdue_with_assessment_after_initial_number_of_days_no_date(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
        )

        # This person started on probation more than LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_nd_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 1)

    def test_us_nd_num_days_assessment_overdue_with_assessment_after_initial_number_of_days_with_date(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=100,
            assessment_date=date(2010, 2, 2),
        )

        # This person started on probation more than LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        # ago, and they do not have a recent assessment, so their assessment is not in compliance
        start_of_supervision = start_date - relativedelta(
            days=LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE
        )
        compliance_evaluation_date = start_date + relativedelta(days=1)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_overdue = us_nd_supervision_compliance._num_days_assessment_overdue(
            compliance_evaluation_date, assessment
        )

        self.assertEqual(days_overdue, 2742)
