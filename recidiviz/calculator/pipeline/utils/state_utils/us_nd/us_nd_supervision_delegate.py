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
"""US_ND implementation of the supervision delegate"""
from typing import List, Optional, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

RELEASE_REASON_RAW_TEXT_TO_SUPERVISION_TYPE = {
    "PRB": StateSupervisionPeriodSupervisionType.PROBATION,
    "RPRB": StateSupervisionPeriodSupervisionType.PROBATION,
    "PARL": StateSupervisionPeriodSupervisionType.PAROLE,
    "PV": StateSupervisionPeriodSupervisionType.PAROLE,
    "RPAR": StateSupervisionPeriodSupervisionType.PAROLE,
}

# Mapping of ND supervision district (level 1) to supervision region (level 2).
LEVEL_1_TO_LEVEL_2_SUPERVISION_LOCATION_MAPPING = {
    "1": "Region 3",
    "2": "Region 2",
    "3": "Region 5",
    "4": "Region 1",
    "5": "Region 6",
    "6": "Region 2",
    "7": "Region 1",
    "8": "Region 2",
    "9": "Region 3",
    "10": "Region 5",
    "11": "Region 4",
    "12": "Region 6",
    "13": "Region 4",
    "14": "Region 2",
    "15": "Region 2",
    "16": "Region 4",
    "17": None,
    "18": "Region 5",
}


class UsNdSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_ND implementation of the supervision delegate"""

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """In US_ND, only LSIR assessments are supported."""
        if assessment_class == StateAssessmentClass.RISK:
            return [StateAssessmentType.LSIR]
        return None

    def get_incarceration_period_supervision_type_at_release(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Calculates the post-incarceration supervision type for US_ND by evaluating the raw text fields associated
        with the release_reason.
        """

        if not incarceration_period.release_date:
            raise ValueError(
                f"No release date for incarceration period {incarceration_period.incarceration_period_id}"
            )

        if not incarceration_period.release_reason:
            raise ValueError(
                f"No release reason for incarceraation period {incarceration_period.incarceration_period_id}"
            )

        # Releases to supervision are always classified as a CONDITIONAL_RELEASE
        if (
            incarceration_period.release_reason
            != StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        ):
            return None

        release_reason_raw_text = incarceration_period.release_reason_raw_text

        if not release_reason_raw_text:
            raise ValueError(
                f"Unexpected empty release_reason_raw_text value for incarceration period "
                f"{incarceration_period.incarceration_period_id}."
            )

        supervision_type = RELEASE_REASON_RAW_TEXT_TO_SUPERVISION_TYPE.get(
            release_reason_raw_text
        )

        if not supervision_type:
            raise ValueError(
                f"Unexpected release_reason_raw_text value {release_reason_raw_text} being mapped to"
                f" {StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE}."
            )

        return supervision_type

    # TODO(#3829): Remove this helper once we've built level 1/level 2 supervision
    #  location distinction directly into our schema and are hydrating both for ND.
    def supervision_location_from_supervision_site(
        self,
        supervision_site: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Retrieves level 1 and level 2 location information from a supervision site."""

        if not supervision_site:
            return None, None

        if supervision_site not in LEVEL_1_TO_LEVEL_2_SUPERVISION_LOCATION_MAPPING:
            raise ValueError(
                f"Found unexpected supervision_site value: {supervision_site}"
            )

        level_1_supervision_location = supervision_site
        level_2_supervision_location = LEVEL_1_TO_LEVEL_2_SUPERVISION_LOCATION_MAPPING[
            level_1_supervision_location
        ]

        return level_1_supervision_location, level_2_supervision_location

    def get_deprecated_supervising_district_external_id(
        self,
        level_1_supervision_location: Optional[str],
        level_2_supervision_location: Optional[str],
    ) -> Optional[str]:
        return level_1_supervision_location
