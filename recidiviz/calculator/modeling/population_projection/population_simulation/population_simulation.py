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
"""Simulation object that models a given policy scenario"""
# pylint: disable=unused-argument

from time import time
from typing import Callable, Dict, Optional, Tuple

import pandas as pd

from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation import (
    SubSimulation,
)


class PopulationSimulation:
    """Control the many sub simulations for one scenario (baseline/control or policy)"""

    def __init__(
        self,
        sub_simulations: Dict[str, SubSimulation],
        population_data: pd.DataFrame,
        projection_time_steps: int,
        first_relevant_time_step: int,
        cross_flow_function: Optional[str],
        override_cross_flow_function: Optional[
            Callable[[pd.DataFrame, int], pd.DataFrame]
        ],
        should_scale_populations: bool,
        validation_transitions_data: Optional[pd.DataFrame] = None,
    ) -> None:
        self.sub_simulations = sub_simulations
        self.population_data = population_data
        self.projection_time_steps = projection_time_steps
        self.current_time_step = first_relevant_time_step
        if override_cross_flow_function is not None:
            self.cross_flow_function = override_cross_flow_function
        else:
            self.cross_flow_function = getattr(
                self, cross_flow_function or "update_attributes_identity"
            )
        self.should_scale_populations = should_scale_populations
        self.validation_transition_data = validation_transitions_data or pd.DataFrame()
        self.population_projections = pd.DataFrame()

    def get_population_projections(self) -> pd.DataFrame:
        return self.population_projections

    def simulate_policies(self) -> pd.DataFrame:
        """Run a population projection and return population counts by year, compartment, and sub-group."""

        start = time()

        # Run the sub simulations for each time_step
        self.step_forward(self.projection_time_steps)

        #  Store the results in one Dataframe
        for simulation_tag, simulation_obj in self.sub_simulations.items():
            sub_population_projection = simulation_obj.get_population_projections()
            sub_population_projection["simulation_group"] = simulation_tag
            self.population_projections = pd.concat(
                [self.population_projections, sub_population_projection]
            )

        print("simulation_time: ", time() - start)

        return self.population_projections

    def step_forward(self, num_time_steps: int) -> None:
        """Steps forward in the projection by some number of steps."""
        for _ in range(num_time_steps):
            for simulation_obj in self.sub_simulations.values():
                simulation_obj.step_forward()
            for simulation_obj in self.sub_simulations.values():
                simulation_obj.create_new_cohort()

            self._cross_flow()

            if self.should_scale_populations:
                self._scale_populations()

            for simulation_obj in self.sub_simulations.values():
                simulation_obj.prepare_for_next_step()

            self.current_time_step += 1

    def _collect_subsimulation_populations(self) -> pd.DataFrame:
        """Helper function for step_forward(). Collects subgroup populations for total population scaling."""
        populations_df = pd.DataFrame()
        for simulation_tag, simulation_obj in self.sub_simulations.items():
            sim_pops = simulation_obj.get_current_populations()
            sim_pops["simulation_group"] = simulation_tag
            populations_df = pd.concat([populations_df, sim_pops])
        return populations_df

    def _cross_flow(self) -> None:
        """Helper function for step_forward. Transfer cohorts between SubSimulations"""
        cross_simulation_flows = pd.DataFrame()
        for simulation_tag, simulation_obj in self.sub_simulations.items():
            simulation_cohorts = simulation_obj.cross_flow()
            simulation_cohorts["simulation_group"] = simulation_tag
            cross_simulation_flows = pd.concat(
                [cross_simulation_flows, simulation_cohorts], sort=True
            )

        unassigned_cohorts = cross_simulation_flows[
            cross_simulation_flows.compartment.isnull()
        ]
        if len(unassigned_cohorts) > 0:
            raise ValueError(
                f"cohorts passed up without compartment: {unassigned_cohorts}"
            )

        cross_simulation_flows = self.cross_flow_function(
            cross_simulation_flows, self.current_time_step
        )
        if cross_simulation_flows.empty:
            raise ValueError("Cross simulation flows cannot be empty")

        # collapse cohorts in the same group with the same start_time_step
        cross_simulation_flows.index.name = "start_time_step"
        cross_simulation_flows = (
            cross_simulation_flows.groupby(
                ["start_time_step", "compartment", "simulation_group"]
            )
            .sum()
            .reset_index(["compartment", "simulation_group"])
        )

        for simulation_tag, simulation_obj in self.sub_simulations.items():
            sub_group_cohorts = cross_simulation_flows[
                cross_simulation_flows.simulation_group == simulation_tag
            ].drop("simulation_group", axis=1)
            simulation_obj.ingest_cross_simulation_cohorts(sub_group_cohorts)

    def _scale_populations(self) -> None:
        """Helper function for step_forward. Scale populations in each compartment to match historical data."""

        disaggregated_population_data = (
            "simulation_group" in self.population_data.columns
            and self.population_data["simulation_group"].notnull().all()
        )

        population_df_sort_indices = ["compartment"]
        if disaggregated_population_data:
            population_df_sort_indices.append("simulation_group")

        time_step_population_data = self.population_data[
            self.population_data.time_step == self.current_time_step
        ]
        time_step_population_data = time_step_population_data.groupby(
            population_df_sort_indices
        ).compartment_population.sum()

        subgroup_populations = self._collect_subsimulation_populations()
        subgroup_populations = subgroup_populations.groupby(
            population_df_sort_indices
        ).compartment_population.sum()

        # reorder simulation df and drop compartments to match population_data indices
        subgroup_populations = subgroup_populations.loc[time_step_population_data.index]

        scale_factors = time_step_population_data / subgroup_populations

        scale_factors = scale_factors.reset_index().rename(
            {"compartment_population": "scale_factor"}, axis=1
        )

        for simulation_tag, simulation_obj in self.sub_simulations.items():
            if disaggregated_population_data:
                simulation_scale_factors = scale_factors[
                    scale_factors["simulation_group"] == simulation_tag
                ].drop("simulation_group", axis=1)
            else:
                simulation_scale_factors = scale_factors
            simulation_obj.scale_cohorts(
                simulation_scale_factors, self.current_time_step
            )

    def get_outflows(self, collapse_compartments: bool = False) -> pd.DataFrame:
        """Return the projected outflows (transitions)"""

        # Generate the outflows df by looping through each sub-simulation & compartment
        outflows_df = pd.DataFrame()
        for simulation_tag, simulation_obj in self.sub_simulations.items():
            for (
                compartment_tag,
                compartment,
            ) in simulation_obj.simulation_compartments.items():
                # Copy the outflows df from the compartment and add the compartment and
                # simulation group information
                compartment_outflows = compartment.outflows.copy()
                compartment_outflows.index.name = "outflow_to"
                compartment_outflows.columns.name = "time_step"
                compartment_outflows = pd.DataFrame(
                    compartment_outflows.stack("time_step"),
                    columns=["cohort_population"],
                )
                compartment_outflows["compartment"] = compartment_tag

                # Add a column for the `simulation_group` from the id dict
                compartment_outflows["simulation_group"] = simulation_tag

                # Append the outflows for this simulation group/compartment
                outflows_df = pd.concat([outflows_df, compartment_outflows])
        if collapse_compartments:
            return (
                outflows_df.reset_index()
                .groupby(["compartment", "outflow_to", "time_step"])[
                    ["cohort_population"]
                ]
                .sum()
            )
        return outflows_df

    def gen_arima_output_df(self) -> pd.DataFrame:
        arima_output_df = pd.DataFrame()
        for simulation_tag, sub_simulation in self.sub_simulations.items():
            output_df_sub = pd.concat(
                {simulation_tag: sub_simulation.gen_arima_output_df()},
                names=["simulation_group"],
            )
            arima_output_df = pd.concat([arima_output_df, output_df_sub])
        return arima_output_df

    def gen_scale_factors_df(self) -> pd.DataFrame:
        scale_factors_df = pd.DataFrame()
        for simulation_tag, simulation_obj in self.sub_simulations.items():
            subgroup_scale_factors = simulation_obj.get_scale_factors()
            subgroup_scale_factors["simulation_group"] = simulation_tag
            scale_factors_df = pd.concat([scale_factors_df, subgroup_scale_factors])
        return scale_factors_df

    def get_data_for_compartment_time_step(
        self, compartment: str, time_step: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        simulation_population = self.population_projections[
            (self.population_projections.compartment == compartment)
            & (self.population_projections.time_step == time_step)
        ]
        historical_population = self.population_data[
            (self.population_data.compartment == compartment)
            & (self.population_data.time_step == time_step)
        ]

        return simulation_population, historical_population

    def gen_population_error(self) -> pd.DataFrame:
        """Returns the error of the population projection."""
        population_error = pd.DataFrame(
            index=self.population_data.time_step.unique(),
            columns=self.population_data.compartment.unique(),
        )

        min_projection_time_step = min(self.population_projections["time_step"])
        for compartment in population_error.columns:
            for time_step in population_error.index:
                if time_step < min_projection_time_step:
                    continue

                (
                    simulation_population,
                    historical_population,
                ) = self.get_data_for_compartment_time_step(compartment, time_step)
                simulation_population = (
                    simulation_population.compartment_population.sum()
                )
                historical_population = (
                    historical_population.compartment_population.sum()
                )

                if simulation_population == 0:
                    raise ValueError(
                        f"Simulation population total for compartment {compartment} and time step {time_step} "
                        "cannot be 0 for validation"
                    )
                if historical_population == 0:
                    raise ValueError(
                        f"Historical population data for compartment {compartment} and time step {time_step} "
                        "cannot be 0 for validation"
                    )

                population_error.loc[time_step, compartment] = (
                    simulation_population - historical_population
                ) / historical_population

        return population_error.sort_index()

    def gen_full_error(self) -> pd.DataFrame:
        """Compile error data from sub-simulations"""
        min_projection_time_step = min(self.population_projections["time_step"])
        population_error = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                [
                    self.population_data.compartment.unique(),
                    range(
                        min_projection_time_step,
                        self.population_data.time_step.max() + 1,
                    ),
                ],
                names=["compartment", "time_step"],
            ),
            columns=["simulation_population", "historical_population", "percent_error"],
        )

        for (compartment, time_step) in population_error.index:

            (
                simulation_population,
                historical_population,
            ) = self.get_data_for_compartment_time_step(compartment, time_step)
            if simulation_population.empty:
                simulation_population = None
            else:
                simulation_population = (
                    simulation_population.compartment_population.sum()
                )

            if historical_population.empty:
                historical_population = None
            else:
                historical_population = (
                    historical_population.compartment_population.sum()
                )

            # Skip compartments that do not have any population data
            if (simulation_population == 0) & (
                (historical_population == 0) | (historical_population is None)
            ):
                continue

            if simulation_population == 0:
                raise ValueError(
                    f"Simulation population total for compartment {compartment} and time step {time_step} "
                    "cannot be 0 for validation"
                )
            if historical_population == 0:
                raise ValueError(
                    f"Historical population data for compartment {compartment} and time step {time_step} "
                    "cannot be 0 for validation"
                )

            if simulation_population is not None and historical_population is not None:
                population_error.loc[
                    (compartment, time_step), "simulation_population"
                ] = simulation_population
                population_error.loc[
                    (compartment, time_step), "historical_population"
                ] = historical_population
                population_error.loc[(compartment, time_step), "percent_error"] = (
                    simulation_population - historical_population
                ) / historical_population

        return population_error.sort_index().dropna()

    def set_cross_flow_function(
        self, cross_flow_function: Callable[[pd.DataFrame, int], pd.DataFrame]
    ) -> None:
        """Set a custom cross flow function."""
        self.cross_flow_function = cross_flow_function

    @staticmethod
    def update_attributes_identity(
        cross_simulation_flows: pd.DataFrame, current_time_step: int
    ) -> pd.DataFrame:
        """
        Change simulation_tag for each row to whatever simulation that cohort should move to in the next time_step.
        identity: all cohorts maintain the same simulation_tag
        """
        return cross_simulation_flows

    @staticmethod
    def update_attributes_age_recidiviz_schema(
        cross_simulation_flows: pd.DataFrame, current_time_step: int
    ) -> pd.DataFrame:
        """
        Change simulation_tag for each row to whatever simulation that cohort should move to in the next time_step.
        recidiviz_schema: assumes use of 'age' disaggregation axis with values that match recidiviz BQ "age" column.
        Should only be used with a monthly time step
        """
        ages = {
            "0-24": "25-29",
            "25-29": "30-34",
            "30-34": "35-39",
            "35-39": "40+",
            "40+": "40+",
        }

        # Only change cohorts that are 5 years since their last change
        transitioners_idx = [
            ((i != current_time_step) & ((i - current_time_step) % 60 == 0))
            for i in cross_simulation_flows.index
        ]

        new_cohorts = cross_simulation_flows.copy()

        new_cohorts.loc[transitioners_idx, "simulation_group"] = new_cohorts.loc[
            transitioners_idx, "simulation_group"
        ].map(ages)

        null_value_indices = new_cohorts[new_cohorts.simulation_group.isnull()].index
        if len(null_value_indices) > 0:
            raise ValueError(
                f"Unrecognized age groups: {cross_simulation_flows[null_value_indices]}"
            )

        return new_cohorts
