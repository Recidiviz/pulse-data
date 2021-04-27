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
"""Implements tests for the progress checkers."""
from datetime import datetime
from unittest.case import TestCase

from recidiviz.case_triage.case_updates.progress_checker import (
    check_case_update_action_progress,
)
from recidiviz.case_triage.case_updates.types import (
    CaseUpdateActionType,
    CaseActionVersionData,
)


class TestProgressCheckers(TestCase):
    """Implements tests for the progress checkers."""

    def test_employment(self) -> None:
        empty_employer = CaseActionVersionData(last_employer=None)
        unemployed = CaseActionVersionData(last_employer="UNEMPLOYED")
        recidiviz = CaseActionVersionData(last_employer="Recidiviz")

        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.FOUND_EMPLOYMENT,
                last_version=empty_employer,
                current_version=unemployed,
            )
        )

        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.FOUND_EMPLOYMENT,
                last_version=empty_employer,
                current_version=recidiviz,
            )
        )

        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.FOUND_EMPLOYMENT,
                last_version=unemployed,
                current_version=recidiviz,
            )
        )

        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA,
                last_version=unemployed,
                current_version=unemployed,
            )
        )

        # CIS updated to Recidiviz
        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA,
                last_version=unemployed,
                current_version=recidiviz,
            )
        )

        # CIS updated to unemployed
        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA,
                last_version=recidiviz,
                current_version=unemployed,
            )
        )

    def test_completed_assessment(self) -> None:
        empty_assessment = CaseActionVersionData(last_recorded_date=None)
        april_first = CaseActionVersionData(last_recorded_date=datetime(2021, 4, 1))
        april_second = CaseActionVersionData(last_recorded_date=datetime(2021, 4, 2))

        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.COMPLETED_ASSESSMENT,
                last_version=empty_assessment,
                current_version=empty_assessment,
            )
        )

        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.COMPLETED_ASSESSMENT,
                last_version=empty_assessment,
                current_version=april_first,
            )
        )

        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.COMPLETED_ASSESSMENT,
                last_version=april_first,
                current_version=april_first,
            )
        )
        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.COMPLETED_ASSESSMENT,
                last_version=april_first,
                current_version=april_second,
            )
        )

        # No change in CIS
        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA,
                last_version=april_first,
                current_version=april_first,
            )
        )

        # Changed
        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA,
                last_version=april_first,
                current_version=april_second,
            )
        )

    def test_contact(self) -> None:
        empty_contact = CaseActionVersionData(last_recorded_date=None)
        april_first = CaseActionVersionData(last_recorded_date=datetime(2021, 4, 1))
        april_second = CaseActionVersionData(last_recorded_date=datetime(2021, 4, 2))

        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.SCHEDULED_FACE_TO_FACE,
                last_version=empty_contact,
                current_version=empty_contact,
            )
        )

        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.SCHEDULED_FACE_TO_FACE,
                last_version=empty_contact,
                current_version=april_first,
            )
        )

        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.SCHEDULED_FACE_TO_FACE,
                last_version=april_first,
                current_version=april_first,
            )
        )
        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.SCHEDULED_FACE_TO_FACE,
                last_version=april_first,
                current_version=april_second,
            )
        )

        # No change in CIS
        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.INCORRECT_CONTACT_DATA,
                last_version=april_first,
                current_version=april_first,
            )
        )

        # Changed
        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.INCORRECT_CONTACT_DATA,
                last_version=april_first,
                current_version=april_second,
            )
        )

    def test_downgrade(self) -> None:
        empty_contact = CaseActionVersionData(last_supervision_level=None)
        low = CaseActionVersionData(last_supervision_level="low")
        high = CaseActionVersionData(last_supervision_level="high")

        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.DOWNGRADE_INITIATED,
                last_version=empty_contact,
                current_version=empty_contact,
            )
        )
        self.assertTrue(
            check_case_update_action_progress(
                CaseUpdateActionType.DOWNGRADE_INITIATED,
                last_version=high,
                current_version=high,
            )
        )

        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.DOWNGRADE_INITIATED,
                last_version=high,
                current_version=low,
            )
        )
        self.assertFalse(
            check_case_update_action_progress(
                CaseUpdateActionType.DOWNGRADE_INITIATED,
                last_version=low,
                current_version=high,
            )
        )

    def test_always_in_progress(self) -> None:
        always_in_progress_types = [
            CaseUpdateActionType.NOT_ON_CASELOAD,
            CaseUpdateActionType.CURRENTLY_IN_CUSTODY,
            CaseUpdateActionType.DISCHARGE_INITIATED,
        ]

        for case_update_action_type in always_in_progress_types:
            self.assertTrue(
                check_case_update_action_progress(
                    case_update_action_type,
                    last_version=CaseActionVersionData(),
                    current_version=CaseActionVersionData(),
                )
            )
