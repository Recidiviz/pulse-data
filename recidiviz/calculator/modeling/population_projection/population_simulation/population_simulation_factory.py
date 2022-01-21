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
from typing import Any, Dict, List

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
from recidiviz.calculator.modeling.population_projection.super_simulation.initializer import (
    SimulationInputData,
    UserInputs,
)
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)


class PopulationSimulationFactory:
    """Handles set-up specific logic of PopulationSimulation."""

    @classmethod
    def build_population_simulation(
        cls,
        user_inputs: UserInputs,
        policy_list: List[SparkPolicy],
        first_relevant_ts: int,
        data_inputs: SimulationInputData,
    ) -> PopulationSimulation:
        """
        Initializes sub-simulations
        `user_inputs`: UserInputs
        'policy_list': List of SparkPolicy objects to use for this simulation
        `first_relevant_ts` should be the ts to start initialization at
            initialization. Default is 2 to ensure long-sentence cohorts are well-populated.
        `data_inputs`: RawDataInputs
        """
        start = time()

        cls._check_inputs_valid(
            data_inputs,
            user_inputs,
            policy_list,
        )

        sub_group_ids_dict = cls._populate_sub_group_ids_dict(
            data_inputs.transitions_data, data_inputs.disaggregation_axes
        )
        if data_inputs.should_initialize_compartment_populations:

            # add to `policy_list` to switch from remaining sentences data to transitions data
            alternate_transition_policies = []
            for group_attributes in sub_group_ids_dict.values():
                disaggregated_microsim_data = data_inputs.microsim_data[
                    (
                        data_inputs.microsim_data[data_inputs.disaggregation_axes]
                        == pd.Series(group_attributes)
                    ).all(axis=1)
                ]

                # add one policy per compartment to switch from remaining sentence data to transitions data
                for full_comp in [
                    i
                    for i in data_inputs.compartments_architecture
                    if data_inputs.compartments_architecture[i] != "shell"
                ]:
                    alternate_transitions = disaggregated_microsim_data[
                        disaggregated_microsim_data.compartment == full_comp
                    ]
                    # Only apply the alternate transitions to this compartment
                    # if they were provided
                    if not alternate_transitions.empty:
                        alternate_transition_policies.append(
                            SparkPolicy(
                                policy_fn=partial(
                                    TransitionTable.use_alternate_transitions_data,
                                    alternate_historical_transitions=alternate_transitions,
                                    retroactive=False,
                                ),
                                spark_compartment=full_comp,
                                sub_population=group_attributes,
                                policy_ts=user_inputs.start_time_step + 1,
                                apply_retroactive=False,
                            )
                        )

                    else:
                        logging.warning(
                            "No alternate transition data for %s %s",
                            group_attributes,
                            full_comp,
                        )

            # Insert the microsim "policies" first in the policy_list so they are
            # applied before other policies on the same time step
            policy_list = alternate_transition_policies + policy_list

        sub_simulations = cls._build_sub_simulations(
            data_inputs,
            user_inputs,
            policy_list,
            first_relevant_ts,
            sub_group_ids_dict,
        )

        # If compartment populations are initialized, the first ts is handled in initialization
        if data_inputs.should_initialize_compartment_populations:
            pop_sim_start_ts = first_relevant_ts + 1
            projection_time_steps = user_inputs.projection_time_steps - 1
        else:
            pop_sim_start_ts = first_relevant_ts
            projection_time_steps = user_inputs.projection_time_steps

        population_simulation = PopulationSimulation(
            sub_simulations=sub_simulations,
            sub_group_ids_dict=sub_group_ids_dict,
            total_population_data=data_inputs.total_population_data,
            projection_time_steps=projection_time_steps,
            first_relevant_ts=pop_sim_start_ts,
            cross_flow_function=user_inputs.cross_flow_function,
            override_cross_flow_function=data_inputs.override_cross_flow_function,
            should_scale_populations=data_inputs.should_scale_populations_after_step,
        )

        # run simulation up to the start_year
        population_simulation.step_forward(
            user_inputs.start_time_step - first_relevant_ts
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
        data_inputs: SimulationInputData,
        user_inputs: UserInputs,
        policy_list: List[SparkPolicy],
        first_relevant_ts: int,
        sub_group_ids_dict: Dict[str, Dict[str, Any]],
    ) -> Dict[str, SubSimulation]:
        """Helper function for initialize_simulation. Initialize one sub simulation per sub-population."""
        sub_simulations = {}

        # reset indices to facilitate unused data tracking
        transitions_data = data_inputs.transitions_data.reset_index(drop=True)
        outflows_data = data_inputs.outflows_data.reset_index(drop=True)
        total_population_data = data_inputs.total_population_data.reset_index(drop=True)

        unused_transitions_data = transitions_data
        unused_outflows_data = outflows_data
        unused_total_population_data = total_population_data
        for sub_group_id in sub_group_ids_dict:
            group_attributes = pd.Series(sub_group_ids_dict[sub_group_id])
            disaggregated_transitions_data = transitions_data[
                (
                    transitions_data[data_inputs.disaggregation_axes]
                    == group_attributes
                ).all(axis=1)
            ]
            disaggregated_outflows_data = outflows_data[
                (
                    outflows_data[data_inputs.disaggregation_axes] == group_attributes
                ).all(axis=1)
            ]

            if data_inputs.should_initialize_compartment_populations:
                disaggregated_total_population_data = total_population_data[
                    (
                        total_population_data[data_inputs.disaggregation_axes]
                        == group_attributes
                    ).all(axis=1)
                ]
                unused_total_population_data = unused_total_population_data.drop(
                    disaggregated_total_population_data.index
                )
                start_cohort_sizes = disaggregated_total_population_data[
                    disaggregated_total_population_data.time_step
                    == user_inputs.start_time_step
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
                compartments_architecture=data_inputs.compartments_architecture,
                user_inputs=user_inputs,
                policy_list=group_policies,
                first_relevant_ts=first_relevant_ts,
                should_single_cohort_initialize_compartments=data_inputs.should_initialize_compartment_populations,
                starting_cohort_sizes=start_cohort_sizes,
            )

        if len(unused_transitions_data) > 0:
            logging.warning(
                "Some transitions data left unused: %s", unused_transitions_data
            )
        if len(unused_outflows_data) > 0:
            logging.warning("Some outflows data left unused: %s", unused_outflows_data)
        if (
            data_inputs.should_initialize_compartment_populations
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
        data_inputs: SimulationInputData,
        user_inputs: UserInputs,
        policy_list: List[SparkPolicy],
    ) -> None:
        """Throws if any of the inputs are invalid."""
        # Check the inputs have the required fields and types
        if (
            not data_inputs.should_initialize_compartment_populations
            and user_inputs.speed_run is None
        ):
            raise ValueError(f"Required user_inputs.speed_run is None: {user_inputs=}")

        if any(not isinstance(policy, SparkPolicy) for policy in policy_list):
            raise ValueError(
                f"Policy list can only include SparkPolicy objects: {policy_list}"
            )

        for axis in data_inputs.disaggregation_axes:
            # Unless starting cohorts are required, total_population_data is allowed to be aggregated
            fully_disaggregated_dfs = [
                data_inputs.outflows_data,
                data_inputs.transitions_data,
            ]
            if data_inputs.should_initialize_compartment_populations:
                fully_disaggregated_dfs.append(data_inputs.total_population_data)

            for df in [data_inputs.outflows_data, data_inputs.transitions_data]:
                if axis not in df.columns:
                    raise ValueError(
                        f"All disagregation axis must be included in the input dataframe columns\n"
                        f"Expected: {data_inputs.disaggregation_axes}, Actual: {df.columns}"
                    )

        if not data_inputs.total_population_data.empty:
            if (
                user_inputs.start_time_step
                not in data_inputs.total_population_data.time_step.values
            ):
                raise ValueError(
                    f"Start time must be included in population data input\n"
                    f"Expected: {user_inputs.start_time_step}, "
                    f"Actual: {data_inputs.total_population_data.time_step.unique()}"
                )
