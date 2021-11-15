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
"""Simulate multiple demographic/age groups"""

from typing import Dict

import pandas as pd

from recidiviz.calculator.modeling.population_projection.full_compartment import (
    FullCompartment,
)
from recidiviz.calculator.modeling.population_projection.shell_compartment import (
    ShellCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_compartment import (
    SparkCompartment,
)


class SubSimulation:
    """Run the population projection for one sub group"""

    def __init__(
        self,
        simulation_compartments: Dict[str, SparkCompartment],
    ) -> None:

        # A DataFrame with total population errors at all time steps with population data
        self.end_ts_scale_factors = pd.DataFrame()

        # A dict of compartment tag (pre-trial, jail, prison, release...) to the corresponding SparkCompartment object
        self.simulation_compartments = simulation_compartments

    def get_error(self, compartment: str = "prison", unit: str = "abs") -> pd.DataFrame:
        return self.simulation_compartments[compartment].get_error(unit=unit)

    def get_scale_factors(self) -> pd.DataFrame:
        return self.end_ts_scale_factors

    def gen_arima_output_df(self) -> pd.DataFrame:
        arima_output_df = pd.concat(
            [
                compartment.gen_arima_output_df()
                for compartment in self.simulation_compartments.values()
                if isinstance(compartment, ShellCompartment)
            ],
            sort=True,
        )
        return arima_output_df

    def step_forward(self) -> None:
        """Run the simulation for one time step"""
        for compartment in self.simulation_compartments.values():
            compartment.step_forward()

    def scale_cohorts(self, scale_factors: pd.DataFrame, ts: int) -> None:
        """Scale cohort sizes to match historical data"""
        if len(scale_factors.compartment.unique()) != len(scale_factors):
            raise ValueError(f"Duplicate compartment scale factors: {scale_factors}")

        for compartment_tag in scale_factors.compartment.unique():
            compartment_obj = self.simulation_compartments[compartment_tag]
            if isinstance(compartment_obj, FullCompartment):
                scale_factor = (
                    scale_factors.loc[scale_factors.compartment == compartment_tag]
                    .iloc[0]
                    .scale_factor
                )
                compartment_obj.scale_cohorts(scale_factor)
                self.end_ts_scale_factors.loc[ts, compartment_tag] = scale_factor

    def create_new_cohort(self) -> None:
        """Create a new cohort from new admissions from other compartments"""
        for compartment in self.simulation_compartments.values():
            if isinstance(compartment, FullCompartment):
                compartment.create_new_cohort()

    def prepare_for_next_step(self) -> None:
        """Prepare all compartments for next step"""
        for compartment in self.simulation_compartments.values():
            compartment.prepare_for_next_step()

    def cross_flow(self) -> pd.DataFrame:
        cohorts_table = pd.DataFrame(columns=["compartment"])
        for compartment_name, compartment_obj in self.simulation_compartments.items():
            if isinstance(compartment_obj, FullCompartment):
                compartment_cohorts = compartment_obj.get_cohort_df()
                compartment_cohorts["compartment"] = compartment_name
                cohorts_table = pd.concat(
                    [cohorts_table, compartment_cohorts], sort=True
                )

        return cohorts_table

    def ingest_cross_simulation_cohorts(
        self, cross_simulation_flows: pd.DataFrame
    ) -> None:
        for compartment_obj in self.simulation_compartments.values():
            if isinstance(compartment_obj, FullCompartment):
                compartment_obj.ingest_cross_simulation_cohorts(cross_simulation_flows)

    def get_population_projections(self) -> pd.DataFrame:
        """Return a DataFrame with the simulation population projections"""
        # combine the results into one DataFrame
        simulation_results = pd.DataFrame()
        for compartment_name, compartment in self.simulation_compartments.items():
            if isinstance(compartment, FullCompartment):
                compartment_results = pd.DataFrame(
                    compartment.get_per_ts_population(), columns=["total_population"]
                )
                compartment_results["compartment"] = compartment_name
                compartment_results["time_step"] = compartment_results.index
                simulation_results = pd.concat(
                    [simulation_results, compartment_results], sort=True
                )

        return simulation_results

    def get_current_populations(self) -> pd.DataFrame:
        """Pull the compartment populations from the current time step."""
        # combine the results into one DataFrame
        compartment_populations = pd.DataFrame(
            columns=["compartment", "total_population"]
        )
        for compartment_name, compartment in self.simulation_compartments.items():
            if isinstance(compartment, FullCompartment):
                compartment_populations = compartment_populations.append(
                    {
                        "compartment": compartment_name,
                        "total_population": compartment.get_current_population(),
                    },
                    ignore_index=True,
                )

        return compartment_populations
