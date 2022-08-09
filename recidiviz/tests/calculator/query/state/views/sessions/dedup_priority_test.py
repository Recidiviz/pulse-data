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
"""Tests enum coverage for various `..._dedup_priority` views used in sessions."""
import unittest

from recidiviz.calculator.query.state.views.sessions.admission_start_reason_dedup_priority import (
    INCARCERATION_START_REASON_ORDERED_PRIORITY,
    SUPERVISION_START_REASON_ORDERED_PRIORITY,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_2_dedup_priority import (
    SPECIALIZED_PURPOSE_FOR_INCARCERATION_ORDERED_PRIORITY,
    SUPERVISION_TYPE_ORDERED_PRIORITY,
)
from recidiviz.calculator.query.state.views.sessions.release_termination_reason_dedup_priority import (
    INCARCERATION_RELEASE_REASON_ORDERED_PRIORITY,
    SUPERVISION_TERMINATION_REASON_ORDERED_PRIORITY,
)
from recidiviz.calculator.query.state.views.sessions.supervision_level_dedup_priority import (
    SUPERVISION_LEVEL_ORDERED_PRIORITY,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)


class AdmissionStartReasonDedupPriorityEnumCoverageTest(unittest.TestCase):
    """Tests full enum coverage for the start reason priority lists in the
    admission_start_reason_dedup_priority view."""

    def test_supervision_start_reason(self) -> None:
        for admission_reason in StateSupervisionPeriodAdmissionReason:
            if admission_reason not in SUPERVISION_START_REASON_ORDERED_PRIORITY:
                raise ValueError(
                    f"Missing {admission_reason} in "
                    f"SUPERVISION_START_REASON_ORDERED_PRIORITY."
                    f"This may happen if there is a new enum that needs to be added to SUPERVISION_START_REASON_ORDERED_PRIORITY."
                )
        for admission_reason in SUPERVISION_START_REASON_ORDERED_PRIORITY:
            if admission_reason not in StateSupervisionPeriodAdmissionReason:
                raise ValueError(
                    f"Missing {admission_reason} in "
                    f"StateSupervisionPeriodAdmissionReason."
                    f"This may happen if sessions has an unsupported enum. This enum needs to be added to the state schema before it is used in sessions."
                )

    def test_incarceration_start_reason(self) -> None:
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            if admission_reason not in INCARCERATION_START_REASON_ORDERED_PRIORITY:
                raise ValueError(
                    f"Missing {admission_reason} in "
                    f"INCARCERATION_START_REASON_ORDERED_PRIORITY."
                    f"This may happen if there is a new enum that needs to be added to INCARCERATION_START_REASON_ORDERED_PRIORITY."
                )
        for admission_reason in INCARCERATION_START_REASON_ORDERED_PRIORITY:
            if admission_reason not in StateIncarcerationPeriodAdmissionReason:
                raise ValueError(
                    f"Missing {admission_reason} in "
                    f"StateIncarcerationPeriodAdmissionReason."
                    f"This may happen if sessions has an unsupported enum. This enum needs to be added to the state schema before it is used in sessions."
                )


class ReleaseTerminationReasonDedupPriorityEnumCoverageTest(unittest.TestCase):
    """Tests full enum coverage for the end reason priority lists in the
    release_termination_reason_dedup_priority view."""

    def test_supervision_termination_reason(self) -> None:
        for termination_reason in StateSupervisionPeriodTerminationReason:
            if (
                termination_reason
                not in SUPERVISION_TERMINATION_REASON_ORDERED_PRIORITY
            ):
                raise ValueError(
                    f"Missing {termination_reason} in "
                    f"SUPERVISION_TERMINATION_REASON_ORDERED_PRIORITY."
                    f"This may happen if there is a new enum that needs to be added to SUPERVISION_TERMINATION_REASON_ORDERED_PRIORITY."
                )
        for termination_reason in SUPERVISION_TERMINATION_REASON_ORDERED_PRIORITY:
            if termination_reason not in StateSupervisionPeriodTerminationReason:
                raise ValueError(
                    f"Missing {termination_reason} in "
                    f"StateSupervisionPeriodTerminationReason."
                    f"This may happen if sessions has an unsupported enum. This enum needs to be added to the state schema before it is used in sessions."
                )

    def test_incarceration_release_reason(self) -> None:
        for release_reason in StateIncarcerationPeriodReleaseReason:
            if release_reason not in INCARCERATION_RELEASE_REASON_ORDERED_PRIORITY:
                raise ValueError(
                    f"Missing {release_reason} in "
                    f"INCARCERATION_RELEASE_REASON_ORDERED_PRIORITY."
                    f"This may happen if there is a new enum that needs to be added to INCARCERATION_RELEASE_REASON_ORDERED_PRIORITY."
                )
        for release_reason in INCARCERATION_RELEASE_REASON_ORDERED_PRIORITY:
            if release_reason not in StateIncarcerationPeriodReleaseReason:
                raise ValueError(
                    f"Missing {release_reason} in "
                    f"StateIncarcerationPeriodReleaseReason."
                    f"This may happen if sessions has an unsupported enum. This enum needs to be added to the state schema before it is used in sessions."
                )


class SupervisionLevelDedupPriorityEnumCoverageTest(unittest.TestCase):
    """Tests full enum coverage for the supervision level priority lists in the
    supervision_level_dedup_priority view."""

    def test_supervision_level(self) -> None:
        for level in StateSupervisionLevel:
            if level == StateSupervisionLevel.INCARCERATED:
                continue
            if level not in SUPERVISION_LEVEL_ORDERED_PRIORITY:
                raise ValueError(
                    f"Missing {level} in "
                    f"SUPERVISION_LEVEL_ORDERED_PRIORITY."
                    f"This may happen if there is a new enum that needs to be added to SUPERVISION_LEVEL_ORDERED_PRIORITY."
                )
        # TODO(#12046): [Pathways] Remove TN-specific raw supervision-level mappings
        # TODO(#7912): Add full enum coverage tests once the temporary supervision
        #  level enums have been added to the state schema
        # for level in SUPERVISION_LEVEL_ORDERED_PRIORITY:
        #     if level not in StateSupervisionLevel:
        #         raise ValueError(
        #             f"Missing {level} in "
        #             f"StateSupervisionLevel."
        #             f"This may happen if sessions has an unsupported enum. This enum needs to be added to the state schema before it is used in sessions."
        #         )


class CompartmentLevel2DedupPriorityEnumCoverageTest(unittest.TestCase):
    """Tests full enum coverage for the compartment level 2 priority lists in the
    compartment_level_2_dedup_priority view."""

    def test_supervision_type(self) -> None:
        for compartment_level_2_type in StateSupervisionPeriodSupervisionType:
            if compartment_level_2_type not in SUPERVISION_TYPE_ORDERED_PRIORITY:
                raise ValueError(
                    f"Missing {compartment_level_2_type} in "
                    f"SUPERVISION_TYPE_ORDERED_PRIORITY."
                    f"This may happen if there is a new enum that needs to be added to SUPERVISION_TYPE_ORDERED_PRIORITY."
                )

        for compartment_level_2_priority in SUPERVISION_TYPE_ORDERED_PRIORITY:
            if (
                compartment_level_2_priority
                not in StateSupervisionPeriodSupervisionType
            ):
                raise ValueError(
                    f"Missing {compartment_level_2_priority} in "
                    f"StateSupervisionPeriodSupervisionType. "
                    f"This may happen if sessions has an unsupported enum. "
                    f"This enum needs to be added to the state schema before it is "
                    f"used in sessions."
                )

    def test_specialized_purpose_for_incarceration(self) -> None:
        for compartment_level_2_type in StateSpecializedPurposeForIncarceration:
            if (
                compartment_level_2_type
                not in SPECIALIZED_PURPOSE_FOR_INCARCERATION_ORDERED_PRIORITY
            ):
                raise ValueError(
                    f"Missing {compartment_level_2_type} in "
                    f"SPECIALIZED_PURPOSE_FOR_INCARCERATION_ORDERED_PRIORITY."
                    f"This may happen if there is a new enum that needs to be added to SPECIALIZED_PURPOSE_FOR_INCARCERATION_ORDERED_PRIORITY."
                )
        for (
            compartment_level_2_priority
        ) in SPECIALIZED_PURPOSE_FOR_INCARCERATION_ORDERED_PRIORITY:
            if (
                compartment_level_2_priority
                not in StateSpecializedPurposeForIncarceration
                and compartment_level_2_priority
                not in StateSupervisionPeriodSupervisionType
            ):
                raise ValueError(
                    f"Missing {compartment_level_2_priority} in "
                    f"StateSpecializedPurposeForIncarceration."
                    f"This may happen if sessions has an unsupported enum. This enum needs to be added to the state schema before it is used in sessions."
                )
