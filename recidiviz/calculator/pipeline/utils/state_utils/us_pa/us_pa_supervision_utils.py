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
"""US_PA-specific implementations of functions related to supervision."""
from typing import Any, Dict, Optional, Tuple

from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


def us_pa_get_supervising_officer_and_location_info_from_supervision_period(
    supervision_period: StateSupervisionPeriod,
    supervision_period_to_agent_associations: Dict[int, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Returns the supervising officer and location associated with the given
    supervision_period."""
    supervising_officer_external_id = None
    level_1_supervision_location = None
    level_2_supervision_location = None

    if not supervision_period.supervision_period_id:
        raise ValueError("Unexpected null supervision_period_id")

    # TODO(#6314): Remove this hack once we've re-run US_PA ingest with the fix.
    #  Replace the contents of this function with:
    #  supervision_site = supervision_period.supervision_site
    #  if supervision_site and supervision_site.count("|") == 2:
    #     # In PA, supervision_site follows format
    #     # "{supervision district}|{supervision suboffice}|{supervision unit org code}"
    #     (
    #         level_2_supervision_location,
    #         level_1_supervision_location,
    #         _org_code,
    #     ) = supervision_site.split("|")
    period_info = supervision_period_to_agent_associations.get(
        supervision_period.supervision_period_id
    )
    if period_info:
        supervision_info = period_info["agent_external_id"]

        if supervision_info and supervision_info.count("#") == 1:
            (
                supervision_site,
                supervising_officer_external_id,
            ) = supervision_info.split("#")

            # If the supervising_officer_external_id is only whitespace, set to None
            if not supervising_officer_external_id.strip():
                supervising_officer_external_id = None

            if supervision_site and supervision_site.count("|") == 2:
                # In PA, supervision_site follows format
                # "{supervision district}|{supervision suboffice}|{supervision unit org code}"
                (
                    level_2_supervision_location,
                    level_1_supervision_location,
                    _org_code,
                ) = supervision_site.split("|")
    return (
        supervising_officer_external_id,
        level_1_supervision_location or None,
        level_2_supervision_location or None,
    )
