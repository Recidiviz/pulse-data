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

from typing import Dict, Any, Tuple, Optional, Callable
from time import time
import pandas as pd

from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation import (
    SubSimulation,
)


class PopulationSimulation:
    """Control the many sub simulations for one scenario (baseline/control or policy)"""

    def __init__(
        self,
        sub_simulations: Dict[str, SubSimulation],
        sub_group_ids_dict: Dict[str, Dict[str, Any]],
        total_population_data: pd.DataFrame,
        projection_time_steps: int,
        first_relevant_ts: int,
        cross_flow_function: Optional[str],
        override_cross_flow_function: Optional[Callable],
        validation_transitions_data: Optional[pd.DataFrame] = None,
    ) -> None:
        self.sub_simulations = sub_simulations
        self.sub_group_ids_dict = sub_group_ids_dict
        self.total_population_data = total_population_data
        self.projection_time_steps = projection_time_steps
        self.current_ts = first_relevant_ts
        if override_cross_flow_function is not None:
            self.cross_flow_function = override_cross_flow_function
        else:
            self.cross_flow_function = getattr(
                self, cross_flow_function or "update_attributes_identity"
            )
        self.validation_transition_data = validation_transitions_data or pd.DataFrame()
        self.population_projections = pd.DataFrame()

    def get_population_projections(self) -> pd.DataFrame:
        return self.population_projections

    def simulate_policies(self) -> pd.DataFrame:
        """Run a population projection and return population counts by year, compartment, and sub-group."""

        start = time()

        # Run the sub simulations for each ts
        self.step_forward(self.projection_time_steps)

        #  Store the results in one Dataframe
        for simulation_group_id, simulation_obj in self.sub_simulations.items():
            sub_population_projection = simulation_obj.get_population_projections()
            sub_population_projection["simulation_group"] = simulation_group_id
            self.population_projections = pd.concat(
                [self.population_projections, sub_population_projection]
            )

        print("simulation_time: ", time() - start)

        return self.population_projections

    def step_forward(self, num_ts: int) -> None:
        """Steps forward in the projection by some number of steps."""
        for _ in range(num_ts):
            for simulation_obj in self.sub_simulations.values():
                simulation_obj.step_forward()
            for simulation_obj in self.sub_simulations.values():
                simulation_obj.create_new_cohort()

            self._cross_flow()

            self._scale_populations()

            for simulation_obj in self.sub_simulations.values():
                simulation_obj.prepare_for_next_step()

            self.current_ts += 1

    def _collect_subsimulation_populations(self) -> pd.DataFrame:
        """Helper function for step_forward(). Collects subgroup populations for total population scaling."""
        disaggregation_axes = list(list(self.sub_group_ids_dict.values())[0].keys())
        populations_df = pd.DataFrame()
        for simulation_id, simulation_attr in self.sub_group_ids_dict.items():
            sim_pops = self.sub_simulations[simulation_id].get_current_populations()
            sim_pops[disaggregation_axes] = pd.Series(simulation_attr)
            populations_df = pd.concat([populations_df, sim_pops])
        return populations_df

    def _cross_flow(self) -> None:
        """Helper function for step_forward. Transfer cohorts between SubSimulations"""
        cross_simulation_flows = pd.DataFrame()
        for sub_group_id, simulation_obj in self.sub_simulations.items():
            simulation_cohorts = simulation_obj.cross_flow()
            simulation_cohorts = self._subgroup_id_to_attributes(
                simulation_cohorts, sub_group_id
            )
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
            cross_simulation_flows, self.current_ts
        )

        cross_simulation_flows = self._attributes_to_subgroup_id(cross_simulation_flows)

        # collapse cohorts in the same group with the same start_ts
        cross_simulation_flows.index.name = "start_ts"
        cross_simulation_flows = (
            cross_simulation_flows.groupby(["start_ts", "compartment", "sub_group_id"])
            .sum()
            .reset_index(["compartment", "sub_group_id"])
        )

        for sub_group_id, simulation_obj in self.sub_simulations.items():
            sub_group_cohorts = cross_simulation_flows[
                cross_simulation_flows.sub_group_id == sub_group_id
            ].drop("sub_group_id", axis=1)
            simulation_obj.ingest_cross_simulation_cohorts(sub_group_cohorts)

    def _subgroup_id_to_attributes(
        self, cohorts_df: pd.DataFrame, simulation_id: str
    ) -> pd.DataFrame:
        """Helper function for _cross_flow(). Adds columns for each disaggregation axis to a cohorts df"""
        output_df = cohorts_df.copy()
        for axis, attribute in self.sub_group_ids_dict[simulation_id].items():
            output_df[axis] = attribute
        return output_df

    def _attributes_to_subgroup_id(self, cohorts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper function for _cross_flow().
        Replaces columns for each disaggregation axis with one column for simulation id.
        """
        output_df = cohorts_df.copy()
        disaggregation_axes = list(self.sub_group_ids_dict.values())[0].keys()
        output_df["sub_group_id"] = output_df[disaggregation_axes].sum(axis=1)
        output_df = output_df.drop(disaggregation_axes, axis=1)
        return output_df

    def _scale_populations(self) -> None:
        """Helper function for step_forward. Scale populations in each compartment to match historical data."""
        disaggregation_axes = list(self.sub_group_ids_dict.values())[0].keys()
        population_disagg_axes = [
            axis
            for axis in disaggregation_axes
            if axis in self.total_population_data.columns
            and self.total_population_data[axis].notnull().all()
        ]
        population_df_sort_indices = ["compartment"] + population_disagg_axes

        ts_population_data = self.total_population_data[
            self.total_population_data.time_step == self.current_ts
        ]
        ts_population_data = (
            ts_population_data.groupby(population_df_sort_indices)
            .sum()
            .total_population
        )

        subgroup_populations = self._collect_subsimulation_populations()
        subgroup_populations = (
            subgroup_populations.groupby(population_df_sort_indices)
            .sum()
            .total_population
        )

        # reorder simulation df and drop compartments to match total_population_data indices
        subgroup_populations = subgroup_populations.loc[ts_population_data.index]

        scale_factors = ts_population_data / subgroup_populations

        scale_factors = scale_factors.reset_index().rename(
            {"total_population": "scale_factor"}, axis=1
        )

        for simulation_id, simulation_attr in self.sub_group_ids_dict.items():
            agg_simulation_attr = {
                i: j for i, j in simulation_attr.items() if i in population_disagg_axes
            }
            simulation_scale_factors = scale_factors[
                (scale_factors[population_disagg_axes] == agg_simulation_attr).all(
                    axis=1
                )
            ].drop(population_disagg_axes, axis=1)
            self.sub_simulations[simulation_id].scale_cohorts(
                simulation_scale_factors, self.current_ts
            )

    def calculate_transition_error(
        self, validation_data: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        validation_data should be a DataFrame with exactly:
            one column per axis of disaggregation, 'time_step', 'count', 'compartment', 'outflow_to'
        """

        self.validation_transition_data = (
            validation_data or self.validation_transition_data
        )

        aggregated_results = self.validation_transition_data.copy()
        aggregated_results["count"] = 0

        for sub_group_id, sub_group_obj in self.sub_simulations.items():
            for (
                compartment_tag,
                compartment,
            ) in sub_group_obj.simulation_compartments.items():
                for ts in set(self.validation_transition_data["time_step"]):
                    index_locator = aggregated_results[
                        aggregated_results["time_step"] == ts
                    ]
                    sub_group_id_dict = self.sub_group_ids_dict[sub_group_id]
                    sub_group_id_dict["compartment"] = compartment_tag
                    for axis in sub_group_id_dict:
                        if axis in index_locator.columns:
                            keepers = [
                                sub_group_id_dict[axis] in i
                                for i in index_locator[axis]
                            ]
                            index_locator = index_locator[keepers]

                    validation_indices = {
                        outflow: index
                        for index, outflow in index_locator["outflow_to"].iteritems()
                    }

                    for outflow in validation_indices:
                        aggregated_results.loc[
                            validation_indices[outflow], "count"
                        ] += compartment.outflows[ts][outflow]
        return aggregated_results

    def gen_arima_output_df(self) -> pd.DataFrame:
        arima_output_df = pd.DataFrame()
        for simulation_group, sub_simulation in self.sub_simulations.items():
            output_df_sub = pd.concat(
                {simulation_group: sub_simulation.gen_arima_output_df()},
                names=["simulation_group"],
            )
            arima_output_df = arima_output_df.append(output_df_sub)
        return arima_output_df

    def gen_scale_factors_df(self) -> pd.DataFrame:
        scale_factors_df = pd.DataFrame()
        for subgroup_name, subgroup_obj in self.sub_simulations.items():
            subgroup_scale_factors = subgroup_obj.get_scale_factors()
            subgroup_scale_factors["sub-group"] = subgroup_name
            scale_factors_df = pd.concat([scale_factors_df, subgroup_scale_factors])
        return scale_factors_df

    def get_data_for_compartment_ts(
        self, compartment: str, ts: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        simulation_population = self.population_projections[
            (self.population_projections.compartment == compartment)
            & (self.population_projections.time_step == ts)
        ]
        historical_population = self.total_population_data[
            (self.total_population_data.compartment == compartment)
            & (self.total_population_data.time_step == ts)
        ]

        return simulation_population, historical_population

    def gen_total_population_error(self) -> pd.DataFrame:
        """Returns the error of the total population projection."""
        total_population_error = pd.DataFrame(
            index=self.total_population_data.time_step.unique(),
            columns=self.total_population_data.compartment.unique(),
        )

        min_projection_ts = min(self.population_projections["time_step"])
        for compartment in total_population_error.columns:
            for ts in total_population_error.index:
                if ts < min_projection_ts:
                    continue

                (
                    simulation_population,
                    historical_population,
                ) = self.get_data_for_compartment_ts(compartment, ts)
                simulation_population = simulation_population.total_population.sum()
                historical_population = historical_population.total_population.sum()

                if simulation_population == 0:
                    raise ValueError(
                        f"Simulation population total for compartment {compartment} and time step {ts} "
                        "cannot be 0 for validation"
                    )
                if historical_population == 0:
                    raise ValueError(
                        f"Historical population data for compartment {compartment} and time step {ts} "
                        "cannot be 0 for validation"
                    )

                total_population_error.loc[ts, compartment] = (
                    simulation_population - historical_population
                ) / historical_population

        return total_population_error.sort_index()

    def gen_full_error(self) -> pd.DataFrame:
        """Compile error data from sub-simulations"""
        min_projection_ts = min(self.population_projections["time_step"])
        total_population_error = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                [
                    self.total_population_data.compartment.unique(),
                    range(
                        min_projection_ts,
                        self.total_population_data.time_step.max() + 1,
                    ),
                ],
                names=["compartment", "time_step"],
            ),
            columns=["simulation_population", "historical_population", "percent_error"],
        )

        for (compartment, ts) in total_population_error.index:

            (
                simulation_population,
                historical_population,
            ) = self.get_data_for_compartment_ts(compartment, ts)
            if simulation_population.empty:
                simulation_population = None
            else:
                simulation_population = simulation_population.total_population.sum()

            if historical_population.empty:
                historical_population = None
            else:
                historical_population = historical_population.total_population.sum()

            if simulation_population == 0:
                raise ValueError(
                    f"Simulation population total for compartment {compartment} and time step {ts} "
                    "cannot be 0 for validation"
                )
            if historical_population == 0:
                raise ValueError(
                    f"Historical population data for compartment {compartment} and time step {ts} "
                    "cannot be 0 for validation"
                )

            if simulation_population is not None and historical_population is not None:
                total_population_error.loc[
                    (compartment, ts), "simulation_population"
                ] = simulation_population
                total_population_error.loc[
                    (compartment, ts), "historical_population"
                ] = historical_population
                total_population_error.loc[(compartment, ts), "percent_error"] = (
                    simulation_population - historical_population
                ) / historical_population

        return total_population_error.sort_index().dropna()

    def set_cross_flow_function(self, cross_flow_function: Callable) -> None:
        """Set a custom cross flow function."""
        self.cross_flow_function = cross_flow_function

    @staticmethod
    def update_attributes_identity(
        cross_simulation_flows: pd.DataFrame, current_ts: int
    ) -> pd.DataFrame:
        """
        Change sub_group_id for each row to whatever simulation that cohort should move to in the next ts.
        identity: all cohorts maintain the same sub_group_id
        """
        return cross_simulation_flows

    @staticmethod
    def update_attributes_age_recidiviz_schema(
        cross_simulation_flows: pd.DataFrame, current_ts: int
    ) -> pd.DataFrame:
        """
        Change sub_group_id for each row to whatever simulation that cohort should move to in the next ts.
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
            ((i != current_ts) & ((i - current_ts) % 60 == 0))
            for i in cross_simulation_flows.index
        ]

        new_cohorts = cross_simulation_flows.copy()

        new_cohorts.loc[transitioners_idx, "age"] = new_cohorts.loc[
            transitioners_idx, "age"
        ].map(ages)

        null_value_indices = new_cohorts[new_cohorts.age.isnull()].index
        if len(null_value_indices) > 0:
            raise ValueError(
                f"Unrecognized age groups: {cross_simulation_flows[null_value_indices]}"
            )

        return new_cohorts
