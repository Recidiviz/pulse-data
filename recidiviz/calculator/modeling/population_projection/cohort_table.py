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

from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    SIG_FIGS,
)


class CohortTable:
    """Store population counts for one cohort of people that enter one category in the same year"""

    def __init__(self, starting_time_step: int) -> None:
        self.cohort_df = pd.DataFrame(
            {starting_time_step - 1: 0.0}, index=[starting_time_step - 1]
        )
        self.cohort_df.index.name = "start_time_step"
        self.cohort_df.columns.name = "simulation_time_step"

    def get_latest_population(self) -> pd.Series:
        return self.cohort_df.iloc[:, -1]

    def get_per_time_step_population(self) -> pd.Series:
        return self.cohort_df.sum(axis=0)

    def append_time_step_end_count(
        self, cohort_sizes: pd.Series, projection_time_step: int
    ) -> None:
        """Append the cohort sizes for the end of the projection time_step"""
        latest_population = self.get_latest_population()
        for time_step in latest_population.index:
            if time_step not in cohort_sizes:
                cohort_sizes.loc[time_step] = 0
                cohort_sizes.sort_index(inplace=True)

        if (round(cohort_sizes, SIG_FIGS) > round(latest_population, SIG_FIGS)).any():
            raise ValueError(
                "Cannot append cohort data that is larger than the latest population\n"
                f"Latest population: {latest_population[cohort_sizes > latest_population]}\n"
                f"Attempting to append: {cohort_sizes[cohort_sizes > latest_population]}"
            )

        if projection_time_step in self.cohort_df.columns:
            raise ValueError(f"Cannot overwrite cohort for time {projection_time_step}")

        self.cohort_df.loc[:, projection_time_step] = cohort_sizes

    def append_cohort(self, cohort_size: float, projection_time_step: int) -> None:
        """Add a new cohort to the bottom of the cohort table"""
        if projection_time_step not in self.cohort_df.columns:
            raise ValueError(
                f"Cannot append cohort with start time {projection_time_step} outside of CohortTable timeline "
                f"{self.cohort_df.columns}"
            )
        if projection_time_step in self.cohort_df.index:
            raise ValueError(f"Cannot overwrite cohort for time {projection_time_step}")
        self.cohort_df = pd.concat(
            [
                self.cohort_df,
                pd.DataFrame(
                    {projection_time_step: [cohort_size]}, index=[projection_time_step]
                ),
            ]
        ).fillna(0)

    def scale_cohort_size(self, scalar: float) -> None:
        if scalar < 0:
            raise ValueError(f"Cannot scale cohort by a negative factor: {scalar}")
        self.cohort_df *= scalar

    def get_cohort_timeline(self, cohort_start_year: int) -> pd.DataFrame:
        return self.cohort_df.loc[cohort_start_year]

    def pop_cohorts(self) -> pd.DataFrame:
        """pop cohort_df for cross-simulation flow"""
        cohort_df = self.cohort_df
        self.cohort_df = pd.DataFrame()
        return cohort_df

    def ingest_cross_simulation_cohorts(
        self, cross_simulation_flows: pd.DataFrame
    ) -> None:
        """ingest new cohort_df from cross-simulation flow"""
        self.cohort_df = cross_simulation_flows
