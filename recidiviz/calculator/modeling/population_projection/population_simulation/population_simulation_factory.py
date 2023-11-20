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
from typing import Dict, List

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
        first_relevant_time_step: int,
        data_inputs: SimulationInputData,
    ) -> PopulationSimulation:
        """
        Initializes sub-simulations
        `user_inputs`: UserInputs
        'policy_list': List of SparkPolicy objects to use for this simulation
        `first_relevant_time_step` should be the time-step to start initialization at
            initialization. Default is 2 to ensure long-sentence cohorts are well-populated.
        `data_inputs`: RawDataInputs
        """
        start = time()

        cls._check_inputs_valid(
            data_inputs,
            user_inputs,
            policy_list,
        )

        simulation_groups = list(
            data_inputs.transitions_data.simulation_group.dropna().unique()
        )

        if data_inputs.should_initialize_compartment_populations:

            # add to `policy_list` to switch from remaining sentences data to transitions data
            alternate_transition_policies = []
            for sub_group in simulation_groups:
                disaggregated_microsim_data = data_inputs.microsim_data[
                    data_inputs.microsim_data.simulation_group == sub_group
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
                                simulation_group=sub_group,
                                policy_time_step=user_inputs.start_time_step + 1,
                                apply_retroactive=False,
                            )
                        )

                    else:
                        logging.warning(
                            "No alternate transition data for %s %s",
                            sub_group,
                            full_comp,
                        )

            # Insert the microsim "policies" first in the policy_list so they are
            # applied before other policies on the same time step
            policy_list = alternate_transition_policies + policy_list

        sub_simulations = cls._build_sub_simulations(
            data_inputs,
            user_inputs,
            policy_list,
            first_relevant_time_step,
            simulation_groups,
        )

        # If compartment populations are initialized, the first time-step is handled in initialization
        if data_inputs.should_initialize_compartment_populations:
            pop_sim_start_time_step = first_relevant_time_step + 1
            projection_time_steps = user_inputs.projection_time_steps - 1
        else:
            pop_sim_start_time_step = first_relevant_time_step
            projection_time_steps = user_inputs.projection_time_steps

        population_simulation = PopulationSimulation(
            sub_simulations=sub_simulations,
            population_data=data_inputs.population_data,
            projection_time_steps=projection_time_steps,
            first_relevant_time_step=pop_sim_start_time_step,
            cross_flow_function=user_inputs.cross_flow_function,
            override_cross_flow_function=data_inputs.override_cross_flow_function,
            should_scale_populations=data_inputs.should_scale_populations_after_step,
        )

        # run simulation up to the start_year
        population_simulation.step_forward(
            user_inputs.start_time_step - first_relevant_time_step
        )

        print("initialization time: ", time() - start)

        return population_simulation

    @classmethod
    def _build_sub_simulations(
        cls,
        data_inputs: SimulationInputData,
        user_inputs: UserInputs,
        policy_list: List[SparkPolicy],
        first_relevant_time_step: int,
        sub_groups: List[str],
    ) -> Dict[str, SubSimulation]:
        """Helper function for initialize_simulation. Initialize one sub simulation per sub-population."""
        sub_simulations = {}

        # reset indices to facilitate unused data tracking
        transitions_data = data_inputs.transitions_data.reset_index(drop=True)
        admissions_data = data_inputs.admissions_data.reset_index(drop=True)
        population_data = data_inputs.population_data.reset_index(drop=True)

        unused_transitions_data = transitions_data
        unused_admissions_data = admissions_data
        unused_population_data = population_data
        for simulation_group in sub_groups:
            disaggregated_transitions_data = transitions_data[
                transitions_data.simulation_group == simulation_group
            ]
            disaggregated_admissions_data = admissions_data[
                admissions_data.simulation_group == simulation_group
            ]

            if data_inputs.should_initialize_compartment_populations:
                disaggregated_population_data = population_data[
                    population_data.simulation_group == simulation_group
                ]
                unused_population_data = unused_population_data.drop(
                    disaggregated_population_data.index
                )
                start_cohort_sizes = disaggregated_population_data[
                    disaggregated_population_data.time_step
                    == user_inputs.start_time_step
                ]
            else:
                start_cohort_sizes = pd.DataFrame()

            unused_transitions_data = unused_transitions_data.drop(
                disaggregated_transitions_data.index
            )
            unused_admissions_data = unused_admissions_data.drop(
                disaggregated_admissions_data.index
            )

            # Select the policies relevant to this simulation group
            group_policies = SparkPolicy.get_sub_population_policies(
                policy_list, simulation_group
            )

            sub_simulations[
                simulation_group
            ] = SubSimulationFactory.build_sub_simulation(
                admissions_data=disaggregated_admissions_data,
                transitions_data=disaggregated_transitions_data,
                compartments_architecture=data_inputs.compartments_architecture,
                user_inputs=user_inputs,
                policy_list=group_policies,
                first_relevant_time_step=first_relevant_time_step,
                should_single_cohort_initialize_compartments=data_inputs.should_initialize_compartment_populations,
                starting_cohort_sizes=start_cohort_sizes,
            )

        # todo: switch order
        if len(unused_transitions_data) > 0:
            logging.warning(
                "Some transitions data left unused: %s", unused_transitions_data
            )
        if len(unused_admissions_data) > 0:
            logging.warning(
                "Some admissions data left unused: %s", unused_admissions_data
            )
        if (
            data_inputs.should_initialize_compartment_populations
            and len(unused_population_data) > 0
        ):
            logging.warning(
                "Some population data left unused: %s",
                unused_population_data,
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

        if data_inputs.should_initialize_compartment_populations:
            if "simulation_group" not in data_inputs.population_data.columns:
                raise ValueError(
                    "`simulation_group` field must be included in the input population dataset if running microsim."
                )

        if not data_inputs.population_data.empty:
            if (
                user_inputs.start_time_step
                not in data_inputs.population_data.time_step.values
            ):
                raise ValueError(
                    f"Start time must be included in population data input\n"
                    f"Expected: {user_inputs.start_time_step}, "
                    f"Actual: {data_inputs.population_data.time_step.unique()}"
                )
