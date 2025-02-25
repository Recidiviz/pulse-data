# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.p
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Base class for the reported value(s) for a Justice Counts custom reporting
frequency."""
import json as JSON
from typing import Any, Dict, Optional, Type, TypeVar

from recidiviz.persistence.database.schema.justice_counts import schema

CustomReportingFrequencyT = TypeVar(
    "CustomReportingFrequencyT", bound="CustomReportingFrequency"
)


class CustomReportingFrequency:
    """An agency's selection of a custom reporting frequency. The `frequency` is either
    MONTHLY or ANNUAL. The starting_month represents the start of an annual reporting
    frequency. Monthly custom reporting frequencies will have a starting_month of None.
    """

    frequency: Optional[schema.ReportingFrequency]
    starting_month: Optional[int]

    def __init__(
        self,
        frequency: Optional[schema.ReportingFrequency] = None,
        starting_month: Optional[int] = None,
    ):
        self.frequency = frequency
        self.starting_month = starting_month

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CustomReportingFrequency):
            return (
                self.frequency == other.frequency
                and self.starting_month == other.starting_month
            )
        return False

    def to_json_str(self) -> str:
        return JSON.dumps(
            {
                "custom_frequency": (
                    self.frequency.value if self.frequency is not None else None
                ),
                "starting_month": self.starting_month,
            }
        )

    @classmethod
    def from_json(
        cls: Type[CustomReportingFrequencyT], json: Dict[str, Any]
    ) -> CustomReportingFrequencyT:
        frequency_json = json.get("custom_frequency")
        starting_month = json.get("starting_month")
        frequency = (
            schema.ReportingFrequency(frequency_json)
            if frequency_json is not None
            else None
        )
        starting_month = (
            int(starting_month)
            if starting_month is not None and frequency is not None
            else None
        )

        return cls(frequency=frequency, starting_month=starting_month)

    @classmethod
    def from_datapoint(
        cls: Type[CustomReportingFrequencyT], datapoint: schema.Datapoint
    ) -> CustomReportingFrequencyT:
        json = JSON.loads(datapoint.value)
        return cls.from_json(json)
