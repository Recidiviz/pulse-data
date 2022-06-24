# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""US_MI specific StateIncarcerationPeriod"""
from typing import Optional

import attr

from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


@attr.s
class UsMiIncarcerationPeriod(StateIncarcerationPeriod):
    """US_MI specific StateIncarcerationPeriod with information about the reporting
    station, which allows us to have more metadata about the housing unit the person's in."""

    reporting_station_id: Optional[str] = attr.ib(default=None)
    reporting_station_name: Optional[str] = attr.ib(default=None)

    @classmethod
    def from_incarceration_period(
        cls,
        period: StateIncarcerationPeriod,
        reporting_station_id: Optional[str] = None,
        reporting_station_name: Optional[str] = None,
    ) -> "UsMiIncarcerationPeriod":
        period_dict = {
            **period.__dict__,
            "reporting_station_id": reporting_station_id,
            "reporting_station_name": reporting_station_name,
        }

        return cls(**period_dict)  # type: ignore
