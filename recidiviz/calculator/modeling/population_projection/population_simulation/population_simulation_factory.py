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
"""Composition object for PopulationSimulation."""
import logging
from functools import partial
from time import time
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from recidiviz.calculator.modeling.population_projection.population_simulation.population_simulation import (
    PopulationSimulation,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation import (
    SubSimulation,
)
from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation_factory import (
    SubSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)


class PopulationSimulationFactory:
    """Handles set-up specific logic of PopulationSimulation."""

    @classmethod
    def build_population_simulation(
        cls,
        outflows_data: pd.DataFrame,
        transitions_data: pd.DataFrame,
        total_population_data: pd.DataFrame,
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
        user_inputs: Dict[str, Any],
        policy_list: List[SparkPolicy],
        first_relevant_ts: int,
        microsim_data: pd.DataFrame,
        should_initialize_compartment_populations: bool,
        should_scale_populations_after_step: bool,
        override_cross_flow_function: Optional[Callable],
    ) -> PopulationSimulation:
        """
        Initializes sub-simulations
        `outflows_data` should be a DataFrame with columns for each sub-group, year, compartment, outflow_to,
            and total_population
        `transitions_data` should be a DataFrame with columns for each dis-aggregation axis, compartment,
            outflow_to, compartment_duration, and total_population
        `total_population_data` should be a DataFrame with columns for each disaggregation axis, compartment,
            time_step and total_population
        `disaggregation_axes` should be names of columns in historical_data that refer to sub-group categorizations
        `user_inputs` should be a dict including:
            'projection_time_steps': number of ts to project forward
            'start_time_step': first ts of projection
            'policy_list': List of SparkPolicy objects to use for this simulation
            'backcast_projection': True if the simulation should be initialized with a backcast projection
            'constant_admissions': True if the admission population total should remain constant for each time step
        `first_relevant_ts` should be the ts to start initialization at
            initialization. Default is 2 to ensure long-sentence cohorts are well-populated.
        `microsim_data` should only be passed if microsim, and should be a DataFrame with real transitions data, whereas
            in that case `transitions_data` will actually be remaining_duration_data
        `should_initialize_compartment_populations` should be True if compartments should initialize with a single
            start cohort
        `should_scale_populations_after_step` should be True if compartment populations should be scaled to match
            total_population_data after each step forward
        `override_cross_flow_function` is an optional override function to use for cross-sub-simulation cohort flows
        """
        start = time()

        cls._check_inputs_valid(
            outflows_data,
            transitions_data,
            total_population_data,
            disaggregation_axes,
            user_inputs,
            policy_list,
            should_initialize_compartment_populations,
        )

        sub_group_ids_dict = cls._populate_sub_group_ids_dict(
            transitions_data, disaggregation_axes
        )
        if should_initialize_compartment_populations:

            # add to `policy_list to switch from remaining sentences data to transitions data
            for group_attributes in sub_group_ids_dict.values():
                disaggregated_microsim_data = microsim_data[
                    (
                        microsim_data[disaggregation_axes]
                        == pd.Series(group_attributes)
                    ).all(axis=1)
                ]

                # add one policy per compartment to switch from remaining sentence data to transitions data
                for full_comp in [
                    i
                    for i in compartments_architecture
                    if compartments_architecture[i] != "shell"
                ]:
                    policy_list.append(
                        SparkPolicy(
                            policy_fn=partial(
                                TransitionTable.use_alternate_transitions_data,
                                alternate_historical_transitions=disaggregated_microsim_data[
                                    disaggregated_microsim_data.compartment == full_comp
                                ],
                                retroactive=False,
                            ),
                            spark_compartment=full_comp,
                            sub_population=group_attributes,
                            policy_ts=user_inputs["start_time_step"] + 1,
                            apply_retroactive=False,
                        )
                    )

        sub_simulations = cls._build_sub_simulations(
            outflows_data,
            transitions_data,
            total_population_data,
            compartments_architecture,
            disaggregation_axes,
            user_inputs,
            policy_list,
            first_relevant_ts,
            sub_group_ids_dict,
            should_initialize_compartment_populations,
        )

        # If compartment populations are initialized, the first ts is handled in initialization
        if should_initialize_compartment_populations:
            pop_sim_start_ts = first_relevant_ts + 1
            projection_time_steps = user_inputs["projection_time_steps"] - 1
        else:
            pop_sim_start_ts = first_relevant_ts
            projection_time_steps = user_inputs["projection_time_steps"]

        population_simulation = PopulationSimulation(
            sub_simulations,
            sub_group_ids_dict,
            total_population_data,
            projection_time_steps,
            pop_sim_start_ts,
            cross_flow_function=user_inputs.get("cross_flow_function"),
            override_cross_flow_function=override_cross_flow_function,
            should_scale_populations=should_scale_populations_after_step,
        )

        # run simulation up to the start_year
        population_simulation.step_forward(
            user_inputs["start_time_step"] - first_relevant_ts
        )

        print("initialization time: ", time() - start)

        return population_simulation

    @classmethod
    def _populate_sub_group_ids_dict(
        cls, transitions_data: pd.DataFrame, disaggregation_axes: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Helper function for initialize_simulation. Populates sub_group_ids_dict so we can recover sub-group properties
        during validation.
        """
        sub_group_ids_dict = {}
        for simulation_group_name, _ in transitions_data.groupby(disaggregation_axes):
            sub_group_id = str(simulation_group_name)

            if len(disaggregation_axes) == 1:
                sub_group_ids_dict[sub_group_id] = {
                    disaggregation_axes[0]: simulation_group_name
                }
            else:
                sub_group_ids_dict[sub_group_id] = {
                    disaggregation_axes[i]: simulation_group_name[i]
                    for i in range(len(disaggregation_axes))
                }

        # Raise an error if the sub_group_ids_dict was not constructed
        if not sub_group_ids_dict:
            raise ValueError(
                f"Could not define sub groups across the disaggregations: {disaggregation_axes}"
            )

        return sub_group_ids_dict

    @classmethod
    def _build_sub_simulations(
        cls,
        outflows_data: pd.DataFrame,
        transitions_data: pd.DataFrame,
        total_population_data: pd.DataFrame,
        simulation_compartments: Dict[str, str],
        disaggregation_axes: List[str],
        user_inputs: Dict[str, Any],
        policy_list: List[SparkPolicy],
        first_relevant_ts: int,
        sub_group_ids_dict: Dict[str, Dict[str, Any]],
        should_initialize_compartment_populations: bool,
    ) -> Dict[str, SubSimulation]:
        """Helper function for initialize_simulation. Initialize one sub simulation per sub-population."""
        sub_simulations = {}

        # reset indices to facilitate unused data tracking
        transitions_data = transitions_data.reset_index(drop=True)
        outflows_data = outflows_data.reset_index(drop=True)
        total_population_data = total_population_data.reset_index(drop=True)

        unused_transitions_data = transitions_data
        unused_outflows_data = outflows_data
        unused_total_population_data = total_population_data
        for sub_group_id in sub_group_ids_dict:
            group_attributes = pd.Series(sub_group_ids_dict[sub_group_id])
            disaggregated_transitions_data = transitions_data[
                (transitions_data[disaggregation_axes] == group_attributes).all(axis=1)
            ]
            disaggregated_outflows_data = outflows_data[
                (outflows_data[disaggregation_axes] == group_attributes).all(axis=1)
            ]

            if should_initialize_compartment_populations:
                disaggregated_total_population_data = total_population_data[
                    (
                        total_population_data[disaggregation_axes] == group_attributes
                    ).all(axis=1)
                ]
                unused_total_population_data = unused_total_population_data.drop(
                    disaggregated_total_population_data.index
                )
                start_cohort_sizes = disaggregated_total_population_data[
                    disaggregated_total_population_data.time_step
                    == user_inputs["start_time_step"]
                ]
            else:
                start_cohort_sizes = pd.DataFrame()

            unused_transitions_data = unused_transitions_data.drop(
                disaggregated_transitions_data.index
            )
            unused_outflows_data = unused_outflows_data.drop(
                disaggregated_outflows_data.index
            )

            # Select the policies relevant to this simulation group
            group_policies = SparkPolicy.get_sub_population_policies(
                policy_list, sub_group_ids_dict[sub_group_id]
            )

            sub_simulations[sub_group_id] = SubSimulationFactory.build_sub_simulation(
                outflows_data=disaggregated_outflows_data,
                transitions_data=disaggregated_transitions_data,
                compartments_architecture=simulation_compartments,
                user_inputs=user_inputs,
                policy_list=group_policies,
                first_relevant_ts=first_relevant_ts,
                should_single_cohort_initialize_compartments=should_initialize_compartment_populations,
                starting_cohort_sizes=start_cohort_sizes,
            )

        if len(unused_transitions_data) > 0:
            logging.warning(
                "Some transitions data left unused: %s", unused_transitions_data
            )
        if len(unused_outflows_data) > 0:
            logging.warning("Some outflows data left unused: %s", unused_outflows_data)
        if (
            should_initialize_compartment_populations
            and len(unused_total_population_data) > 0
        ):
            logging.warning(
                "Some total population data left unused: %s",
                unused_total_population_data,
            )

        return sub_simulations

    @classmethod
    def _check_inputs_valid(
        cls,
        outflows_data: pd.DataFrame,
        transitions_data: pd.DataFrame,
        total_population_data: pd.DataFrame,
        disaggregation_axes: List[str],
        user_inputs: Dict[str, Any],
        policy_list: List[SparkPolicy],
        should_initialize_compartment_populations: bool,
    ) -> None:
        """Throws if any of the inputs are invalid."""
        # Check the inputs have the required fields and types
        required_user_inputs = [
            "projection_time_steps",
            "start_time_step",
            "constant_admissions",
        ]
        if not should_initialize_compartment_populations:
            required_user_inputs += ["speed_run"]
        missing_inputs = [key for key in required_user_inputs if key not in user_inputs]
        if len(missing_inputs) != 0:
            raise ValueError(f"Required user input are missing: {missing_inputs}")

        if any(not isinstance(policy, SparkPolicy) for policy in policy_list):
            raise ValueError(
                f"Policy list can only include SparkPolicy objects: {policy_list}"
            )

        for axis in disaggregation_axes:
            # Unless starting cohorts are required, total_population_data is allowed to be aggregated
            fully_disaggregated_dfs = [outflows_data, transitions_data]
            if should_initialize_compartment_populations:
                fully_disaggregated_dfs.append(total_population_data)

            for df in [outflows_data, transitions_data]:
                if axis not in df.columns:
                    raise ValueError(
                        f"All disagregation axis must be included in the input dataframe columns\n"
                        f"Expected: {disaggregation_axes}, Actual: {df.columns}"
                    )

        if not total_population_data.empty:
            if (
                user_inputs["start_time_step"]
                not in total_population_data.time_step.values
            ):
                raise ValueError(
                    f"Start time must be included in population data input\n"
                    f"Expected: {user_inputs['start_time_step']}, "
                    f"Actual: {total_population_data.time_step.unique()}"
                )
