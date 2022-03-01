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
"""Custom enum parser tests for US_ME direct ingest."""

import unittest
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.ingest.direct.regions.us_me.us_me_custom_enum_parsers import (
    DOC_FACILITY_LOCATION_TYPES,
    FURLOUGH_MOVEMENT_TYPES,
    OTHER_JURISDICTION_STATUSES,
    SUPERVISION_PRECEDING_INCARCERATION_STATUSES,
    SUPERVISION_STATUSES,
    SUPERVISION_VIOLATION_TRANSFER_REASONS,
    parse_admission_reason,
    parse_deciding_body_type,
    parse_release_reason,
    parse_supervision_violation_response_decision,
    parse_supervision_violation_response_type,
)


class UsMeCustomEnumParsersTest(unittest.TestCase):
    """Parser unit tests for the various US_ME custom enum parsers."""

    ######################################
    # Release Reasons Custom Parser
    ######################################
    @staticmethod
    def _build_release_reason_raw_text(
        current_status: Optional[str] = "NONE",
        next_status: Optional[str] = "NONE",
        next_movement_type: Optional[str] = "NONE",
        transfer_reason: Optional[str] = "NONE",
        location_type: Optional[str] = "NONE",
        next_location_type: Optional[str] = "NONE",
    ) -> str:
        return (
            f"{current_status}@@{next_status}@@{next_movement_type}"
            f"@@{transfer_reason}@@{location_type}@@{next_location_type}"
        )

    def test_parse_release_reason_sentence_served(self) -> None:
        # Next movement type is Discharge
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_movement_type="Discharge"
        )
        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            parse_release_reason(release_reason_raw_text),
        )

    def test_parse_release_reason_escape(self) -> None:
        # Next status is Escape
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_status="Escape"
        )
        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.ESCAPE,
            parse_release_reason(release_reason_raw_text),
        )

        # Next movement type is Escape
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_movement_type="Escape"
        )
        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.ESCAPE,
            parse_release_reason(release_reason_raw_text),
        )

    def test_parse_release_reason_temporary_custody(self) -> None:
        # Current status is County Jail and location type is a DOC Facility
        for location_type in DOC_FACILITY_LOCATION_TYPES:
            release_reason_raw_text = self._build_release_reason_raw_text(
                current_status="County Jail", location_type=location_type
            )
            self.assertEqual(
                StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                parse_release_reason(release_reason_raw_text),
            )

        # Current status is County Jail and location type is not a DOC Facility
        release_reason_raw_text = self._build_release_reason_raw_text(
            current_status="County Jail", location_type="9"
        )
        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            parse_release_reason(release_reason_raw_text),
        )

        # Transfer reason is temporary custody
        release_reason_raw_text = self._build_release_reason_raw_text(
            transfer_reason="Safe Keepers",
        )
        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            parse_release_reason(release_reason_raw_text),
        )

    def test_parse_release_reason_transfer_jurisdiction(self) -> None:
        # Next status is either incarceration or other jurisdiction and next location is not a DOC Facility
        for next_status in [
            "Incarcerated",
            "County Jail",
        ] + OTHER_JURISDICTION_STATUSES:
            release_reason_raw_text = self._build_release_reason_raw_text(
                next_status=next_status, location_type="13"
            )
            self.assertEqual(
                StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
                parse_release_reason(release_reason_raw_text),
            )

    def test_parse_release_reason_temporary_release(self) -> None:
        # Next movement type is Furlough or Furlough Hospital
        for next_movement_type in ["Furlough", "Furlough Hospital"]:
            release_reason_raw_text = self._build_release_reason_raw_text(
                next_movement_type=next_movement_type
            )
            self.assertEqual(
                StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
                parse_release_reason(release_reason_raw_text),
            )

    def test_parse_release_reason_transfer(self) -> None:
        # Next movement type is Transfer
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_movement_type="Transfer"
        )
        self.assertEqual(
            StateIncarcerationPeriodReleaseReason.TRANSFER,
            parse_release_reason(release_reason_raw_text),
        )

    ######################################
    # Admission Reasons Custom Parser
    ######################################
    @staticmethod
    def _build_admission_reason_raw_text(
        previous_status: Optional[str] = "NONE",
        current_status: Optional[str] = "NONE",
        movement_type: Optional[str] = "NONE",
        transfer_type: Optional[str] = "NONE",
        transfer_reason: Optional[str] = "NONE",
        location_type: Optional[str] = "NONE",
    ) -> str:
        return (
            f"{previous_status}@@{current_status}@@{movement_type}@@{transfer_type}"
            f"@@{transfer_reason}@@{location_type}"
        )

    def test_parse_admission_reason_new_admission(self) -> None:
        # Transfer reason is Sentence/Disposition
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Sentence/Disposition"
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            parse_admission_reason(admission_reason_raw_text),
        )

    def test_parse_admission_reason_new_admission_movement(self) -> None:
        # Movement type is Sentence/Disposition
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            movement_type="Sentence/Disposition"
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            parse_admission_reason(admission_reason_raw_text),
        )

    def test_parse_admission_reason_revocation(self) -> None:
        # Next status is supervision
        for supervision_status in (
            SUPERVISION_STATUSES + SUPERVISION_PRECEDING_INCARCERATION_STATUSES
        ):
            admission_reason_raw_text = self._build_admission_reason_raw_text(
                transfer_reason="Sentence/Disposition",
                previous_status=supervision_status,
            )
            self.assertEqual(
                StateIncarcerationPeriodAdmissionReason.REVOCATION,
                parse_admission_reason(admission_reason_raw_text),
            )

    def test_parse_admission_reason_temporary_custody(self) -> None:
        # Current status is County Jail and location type is a DOC Facility
        for location_type in DOC_FACILITY_LOCATION_TYPES:
            admission_reason_raw_text = self._build_admission_reason_raw_text(
                current_status="County Jail", location_type=location_type
            )
            self.assertEqual(
                StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                parse_admission_reason(admission_reason_raw_text),
            )

        # Transfer reason is temporary custody
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Safe Keepers",
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            parse_admission_reason(admission_reason_raw_text),
        )

    def test_parse_admission_reason_transfer_jurisdiction(self) -> None:
        # Transfer type is out of other jurisdiction transfer type
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_type="Non-DOC In"
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
            parse_admission_reason(admission_reason_raw_text),
        )

        # Transfer reason is Other Jurisdiction
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Other Jurisdiction"
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
            parse_admission_reason(admission_reason_raw_text),
        )

        # Previous status from other jurisdiction
        for previous_status in OTHER_JURISDICTION_STATUSES:
            admission_reason_raw_text = self._build_admission_reason_raw_text(
                previous_status=previous_status
            )
            self.assertEqual(
                StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
                parse_admission_reason(admission_reason_raw_text),
            )

    def test_parse_admission_reason_temporary_release(self) -> None:
        # Movement type is Furlough or Furlough Hospital
        for movement_type in FURLOUGH_MOVEMENT_TYPES:
            admission_reason_raw_text = self._build_admission_reason_raw_text(
                movement_type=movement_type
            )
            self.assertEqual(
                StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
                parse_admission_reason(admission_reason_raw_text),
            )

    def test_parse_admission_reason_escape(self) -> None:
        # Previous status or movement type is Escape
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            movement_type="Escape"
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
            parse_admission_reason(admission_reason_raw_text),
        )

        admission_reason_raw_text = self._build_admission_reason_raw_text(
            previous_status="Escape"
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
            parse_admission_reason(admission_reason_raw_text),
        )

    def test_parse_admission_reason_revocation_no_previous_status(self) -> None:
        # previous_status is NULL and transfer_reason is a violation reason
        for violation_reason in SUPERVISION_VIOLATION_TRANSFER_REASONS:
            admission_reason_raw_text = self._build_admission_reason_raw_text(
                transfer_reason=violation_reason
            )
            self.assertEqual(
                StateIncarcerationPeriodAdmissionReason.REVOCATION,
                parse_admission_reason(admission_reason_raw_text),
            )

    def test_parse_admission_reason_transfer(self) -> None:
        # Next movement type is Transfer
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            movement_type="Transfer"
        )
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.TRANSFER,
            parse_admission_reason(admission_reason_raw_text),
        )

    def test_parse_supervision_violation_response_decision_full_revocation(
        self,
    ) -> None:
        raw_text = "Violation Found@@Full Revocation"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.REVOCATION, parsed_decision
        )

    def test_parse_supervision_violation_response_decision_partial_revocation(
        self,
    ) -> None:
        raw_text = "Violation Found@@Partial Revocation - with termination"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.REVOCATION, parsed_decision
        )

    def test_parse_supervision_violation_response_decision_new_conditions(self) -> None:
        raw_text = "Violation Found@@Violation Found - Conditions Amended"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.NEW_CONDITIONS, parsed_decision
        )

    def test_parse_supervision_violation_response_decision_no_sanction(self) -> None:
        raw_text = "Violation Found@@Violation Found - No Sanction"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.NO_SANCTION, parsed_decision
        )

    def test_parse_supervision_violation_response_decision_unfounded(self) -> None:
        raw_text = "Violation Not Found@@NONE"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED,
            parsed_decision,
        )

    def test_parse_supervision_violation_response_decision_warning(self) -> None:
        raw_text = "Warning By Officer@@NONE"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.WARNING, parsed_decision
        )

    def test_parse_supervision_violation_response_decision_graduated_sanction(
        self,
    ) -> None:
        raw_text = "Graduated Sanction By Officer@@NONE"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.OTHER, parsed_decision
        )

    def test_parse_supervision_violation_response_decision_roundabout_revocation(
        self,
    ) -> None:
        raw_text = "Return to Facility By Officer@@NONE"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.REVOCATION, parsed_decision
        )

    def test_parse_supervision_violation_response_decision_other(self) -> None:
        raw_text = "Violation Found@@NONE"
        parsed_decision = parse_supervision_violation_response_decision(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN, parsed_decision
        )

    def test_parse_supervision_violation_response_type_full_revocation(self) -> None:
        raw_text = "Violation Found@@Full Revocation"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.PERMANENT_DECISION, parsed_decision
        )

    def test_parse_supervision_violation_response_type_partial_revocation(self) -> None:
        raw_text = "Violation Found@@Partial Revocation - with termination"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.PERMANENT_DECISION, parsed_decision
        )

    def test_parse_supervision_violation_response_type_new_conditions(self) -> None:
        raw_text = "Violation Found@@Violation Found - Conditions Amended"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.PERMANENT_DECISION, parsed_decision
        )

    def test_parse_supervision_violation_response_type_no_sanction(self) -> None:
        raw_text = "Violation Found@@Violation Found - No Sanction"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.VIOLATION_REPORT, parsed_decision
        )

    def test_parse_supervision_violation_response_type_unfounded(self) -> None:
        raw_text = "Violation Not Found@@NONE"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertIsNone(parsed_decision)

    def test_parse_supervision_violation_response_type_warning(self) -> None:
        raw_text = "Warning By Officer@@NONE"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.CITATION, parsed_decision
        )

    def test_parse_supervision_violation_response_type_graduated_sanction(self) -> None:
        raw_text = "Graduated Sanction By Officer@@NONE"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.PERMANENT_DECISION, parsed_decision
        )

    def test_parse_supervision_violation_response_type_roundabout_revocation(
        self,
    ) -> None:
        raw_text = "Return to Facility By Officer@@NONE"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.PERMANENT_DECISION, parsed_decision
        )

    def test_parse_supervision_violation_response_type_other(self) -> None:
        raw_text = "Violation Found@@NONE"
        parsed_decision = parse_supervision_violation_response_type(raw_text)

        self.assertEqual(
            StateSupervisionViolationResponseType.VIOLATION_REPORT, parsed_decision
        )

    def test_parse_deciding_body_type_violation_finding_court(self) -> None:
        court_violation_findings = [
            "ABSCONDED - FACILITY NOTIFIED",
            "DISMISSED BY COURT",
            "NOT APPROVED BY PROSECUTING ATTORNEY",
            "VIOLATION NOT FOUND",
            "VIOLATION FOUND",
        ]
        for violation_finding in court_violation_findings:
            self.assertEqual(
                StateSupervisionViolationResponseDecidingBodyType.COURT,
                parse_deciding_body_type(f"{violation_finding}@@NONE"),
            )

    def test_parse_deciding_body_type_violation_finding_officer(self) -> None:
        officer_violation_findings = [
            "GRADUATED SANCTION BY OFFICER",
            "RETURN TO FACILITY BY OFFICER",
            "WARNING BY OFFICER",
            "WITHDRAWN BY OFFICER",
        ]
        for violation_finding in officer_violation_findings:
            self.assertEqual(
                StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
                parse_deciding_body_type(f"{violation_finding}@@NONE"),
            )

    def test_parse_deciding_body_type_disposition_only(self) -> None:
        disposition_description = "VIOLATION FOUND - NO SANCTION"
        self.assertEqual(
            StateSupervisionViolationResponseDecidingBodyType.COURT,
            parse_deciding_body_type(f"NONE@@{disposition_description}"),
        )
