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
"""Encapsulate the population data per cohort and time step"""

import pandas as pd


class CohortTable:
    """Store population counts for one cohort of people that enter one category in the same year"""

    def __init__(self, starting_ts: int, transition_table_max_length: int):
        self.cohort_df = pd.DataFrame(dtype=float, index=range(starting_ts - transition_table_max_length, starting_ts))

        self.retired_cohort_df = pd.DataFrame()

    def get_latest_population(self) -> pd.Series:
        if self.cohort_df.empty:
            return pd.Series({start_ts: 0 for start_ts in self.cohort_df.index})

        return self.cohort_df.iloc[:, -1]

    def get_per_ts_population(self) -> pd.Series:
        return self.cohort_df.sum(axis=0)

    def append_ts_end_count(self, cohort_sizes: pd.Series, projection_ts: int) -> None:
        """Append the cohort sizes for the end of the projection ts"""
        latest_population = self.get_latest_population()
        if (cohort_sizes > latest_population).any():
            raise ValueError("Cannot append cohort data that is larger than the latest population\n"
                             f"Latest population: {latest_population}\n"
                             f"Attempting to append: {cohort_sizes}")

        if projection_ts in self.cohort_df.columns:
            raise ValueError(f"Cannot overwrite cohort for time {projection_ts}")

        self.cohort_df[projection_ts] = cohort_sizes

    def append_cohort(self, cohort_size: float, projection_ts: int) -> None:
        """Add a new cohort to the bottom of the cohort table"""
        if projection_ts not in self.cohort_df.columns:
            raise ValueError(f"Cannot append cohort with start time {projection_ts} outside of CohortTable timeline "
                             f"{self.cohort_df.columns}")
        if projection_ts in self.cohort_df.index:
            raise ValueError(f"Cannot overwrite cohort for time {projection_ts}")
        self.cohort_df = \
            self.cohort_df.append(pd.DataFrame({projection_ts: [cohort_size]}, index=[projection_ts])).fillna(0)

    def scale_cohort_size(self, scalar: float) -> None:
        if scalar < 0:
            raise ValueError(f"Cannot scale cohort by a negative factor: {scalar}")
        self.cohort_df *= scalar

    def get_cohort_timeline(self, cohort_start_year: int) -> pd.DataFrame:
        return self.cohort_df.loc[cohort_start_year]

    def pop_cohorts(self):
        """pop cohort_df for cross-simulation flow"""
        cohort_df = self.cohort_df
        self.cohort_df = pd.DataFrame()
        return cohort_df

    def ingest_cross_simulation_cohorts(self, cross_simulation_flows: pd.DataFrame):
        """ingest new cohort_df from cross-simulation flow"""
        self.cohort_df = cross_simulation_flows
