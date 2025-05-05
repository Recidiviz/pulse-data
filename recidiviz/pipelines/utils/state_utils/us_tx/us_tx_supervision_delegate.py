# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Contains generic implementation of the StateSpecificSupervisionDelegate."""
from typing import List, Optional

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)


class UsTxSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_TX implementation of the StateSpecificSupervisionDelegate."""

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """For unit tests, we support all types of assessments."""
        if assessment_class == StateAssessmentClass.RISK:
            return [
                StateAssessmentType.LSIR,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            ]
        return None

    # TODO(#19343) When level 1 and 2 locations are deprecated and/or moved into the schema,
    # ensure the downstream uses of TX data are accounted for.
    def supervision_location_from_supervision_site(
        self,
        supervision_site: Optional[str],
    ) -> tuple[Optional[str], Optional[str]]:
        """
        This method takes in the supervision_site for a supervision period and returns
        the "location level 1" and "location level 2" for that location.

        Location level 1 is the "supervision office" in sessions, which is the given
        location external ID.

        Location level 2 is the "supervision district", which is the REGION of the
        office in TX raw data. Since the location external ID is REGION-DISTRICT,
        this is just the first half of the ID.
        """
        # Turns an empty string to None, because we should not have empty strings.
        if not supervision_site:
            return None, None
        # TX Regions is our state-agnostic "district" level.
        region, _district = supervision_site.split("-")
        return supervision_site, region
