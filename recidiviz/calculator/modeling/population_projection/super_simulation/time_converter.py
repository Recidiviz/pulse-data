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
"""SuperSimulation composed object for tracking different units of time."""
from datetime import datetime

import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    SIG_FIGS,
)


class TimeConverter:
    """Manage time conversions."""

    def __init__(
        self,
        reference_year: float,
        time_step: float,
    ) -> None:
        self.reference_year = reference_year
        self.time_step = time_step

    def convert_year_to_time_step(self, year: float) -> int:
        """converts units of years to units of time steps"""
        ts = (year - self.reference_year) / self.time_step
        if abs(ts - round(ts)) > 0.01:
            raise ValueError(f"Cannot convert year {year} to time step {ts}")
        return round(ts)

    def convert_time_steps_to_year(self, time_steps: pd.Series) -> pd.Series:
        """converts a number of time steps relative to reference date into absolute dates"""
        return time_steps.map(
            lambda x: np.round(x * self.time_step + self.reference_year, SIG_FIGS)
        )

    def convert_timestamp_to_time_step(self, timestamp: datetime) -> int:
        """Converts units of datetimes to units of time steps"""
        if not np.isclose(self.time_step, 1 / 12):
            raise ValueError(
                "Population projection does not currently support simulations with datetime data and non-monthly time step"
            )

        reference_date_year = np.floor(self.reference_year)
        reference_date_month = TimeConverter.get_month_from_year(self.reference_year)
        ts = (
            12 * (timestamp.year - reference_date_year)
            + timestamp.month
            - reference_date_month
        )
        if abs(ts - round(ts)) > 0.01:
            raise ValueError(f"Cannot convert date {timestamp} to integer {ts}")
        return round(ts)

    def convert_time_steps_to_timestamp(self, time_steps: pd.Series) -> pd.Series:
        """Converts a Series of relative time steps into a Series of datetimes"""
        timestamp_df = pd.DataFrame(
            {"time_step": round(self.convert_time_steps_to_year(time_steps), 5)}
        )
        timestamp_df["month"] = timestamp_df["time_step"].map(
            TimeConverter.get_month_from_year
        )
        timestamp_df["year"] = timestamp_df["time_step"].astype(int)
        timestamp_df["day"] = 1
        return pd.to_datetime(timestamp_df[["year", "month", "day"]])

    def get_num_time_steps(self, years: float) -> int:
        """Returns the number of time steps that divide into |years|. Throws if the time step doesn't divide evenly"""
        return self.convert_year_to_time_step(years) - self.convert_year_to_time_step(
            0.0
        )

    def get_time_step(self) -> float:
        return self.time_step

    @staticmethod
    def get_month_from_year(year: float) -> int:
        """Return the month calendar value from the floating point `year` value"""
        return int(round(12 * (year % 1))) + 1
