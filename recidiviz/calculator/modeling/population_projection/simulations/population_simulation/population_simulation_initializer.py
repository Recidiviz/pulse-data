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

from abc import ABC, abstractmethod
from typing import Dict, List, Any
from warnings import warn
import pandas as pd

from recidiviz.calculator.modeling.population_projection.simulations.sub_simulation.sub_simulation_factory \
    import SubSimulationFactory
from recidiviz.calculator.modeling.population_projection.simulations.sub_simulation.sub_simulation import SubSimulation

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class PopulationSimulationInitializer(ABC):
    """Handles set-up specific logic of PopulationSimulation."""

    def __init__(self,
                 should_initialize_compartment_populations: bool,
                 should_scale_populations_after_step: bool,
    ):
        self.should_initialize_compartment_populations = should_initialize_compartment_populations
        self.should_scale_populations_after_step = should_scale_populations_after_step

    @abstractmethod
    def initialize_simulation(self, outflows_data: pd.DataFrame, transitions_data: pd.DataFrame,
                              total_population_data: pd.DataFrame,
                              simulation_compartments: Dict[str, str],
                              disaggregation_axes: List[str], user_inputs: Dict, first_relevant_ts: int,
                              sub_group_ids_dict: Dict[str, Dict[str, Any]],
                              microsim_data: pd.DataFrame, sub_simulations: Dict[str, SubSimulation]) -> None:
        """Initializes sub-simulations"""
        self._populate_sub_group_ids_dict(transitions_data, disaggregation_axes, sub_group_ids_dict)

    @classmethod
    def _populate_sub_group_ids_dict(cls, transitions_data: pd.DataFrame, disaggregation_axes: List[str],
                                     sub_group_ids_dict: Dict[str, Dict[str, Any]]) -> None:
        """Helper function for initialize_simulation"""
        # populate self.sub_group_ids_dict so we can recover sub-group properties during validation
        for simulation_group_name, _ in transitions_data.groupby(disaggregation_axes):
            sub_group_id = str(simulation_group_name)

            if len(disaggregation_axes) == 1:
                sub_group_ids_dict[sub_group_id] = {disaggregation_axes[0]: simulation_group_name}
            else:
                sub_group_ids_dict[sub_group_id] = \
                    {disaggregation_axes[i]: simulation_group_name[i] for i in range(len(disaggregation_axes))}

        # Raise an error if the sub_group_ids_dict was not constructed
        if not sub_group_ids_dict:
            raise ValueError(f"Could not define sub groups across the disaggregations: {disaggregation_axes}")

    def _populate_sub_simulations(self, outflows_data: pd.DataFrame,
                                  transitions_data: pd.DataFrame,
                                  total_population_data: pd.DataFrame,
                                  simulation_compartments: Dict[str, str],
                                  disaggregation_axes: List[str],
                                  user_inputs: Dict,
                                  first_relevant_ts: int,
                                  sub_group_ids_dict: Dict[str, Dict[str, Any]],
                                  sub_simulations: Dict[str, SubSimulation]) -> None:
        """Helper function for initialize_simulation"""

        # reset indices to facilitate unused data tracking
        transitions_data = transitions_data.reset_index(drop=True)
        outflows_data = outflows_data.reset_index(drop=True)
        total_population_data = total_population_data.reset_index(drop=True)

        # Initialize one sub simulation per sub-population
        unused_transitions_data = transitions_data
        unused_outflows_data = outflows_data
        unused_total_population_data = total_population_data
        for sub_group_id in sub_group_ids_dict:
            group_attributes = pd.Series(sub_group_ids_dict[sub_group_id])
            disaggregated_transitions_data = \
                transitions_data[(transitions_data[disaggregation_axes] == group_attributes).all(axis=1)]
            disaggregated_outflows_data = \
                outflows_data[(outflows_data[disaggregation_axes] == group_attributes).all(axis=1)]
            disaggregated_total_population_data = \
                total_population_data[(total_population_data[disaggregation_axes] == group_attributes).all(axis=1)]

            unused_transitions_data = unused_transitions_data.drop(disaggregated_transitions_data.index)
            unused_outflows_data = unused_outflows_data.drop(disaggregated_outflows_data.index)
            unused_total_population_data = unused_total_population_data.drop(disaggregated_total_population_data.index)

            # Select the policies relevant to this simulation group
            group_policies = SparkPolicy.get_sub_population_policies(user_inputs['policy_list'],
                                                                     sub_group_ids_dict[sub_group_id])

            sub_simulations[sub_group_id] = SubSimulationFactory.build_sub_simulation(
                outflows_data=disaggregated_outflows_data,
                transitions_data=disaggregated_transitions_data,
                total_population_data=disaggregated_total_population_data,
                compartments_architecture=simulation_compartments,
                user_inputs=user_inputs,
                policy_list=group_policies,
                first_relevant_ts=first_relevant_ts,
                should_single_cohort_initialize_compartments=self.should_initialize_compartment_populations,
                should_scale_populations_after_step=self.should_scale_populations_after_step
            )

        if len(unused_transitions_data) > 0:
            warn(f"Some transitions data left unused: {unused_transitions_data}", Warning)
        if len(unused_outflows_data) > 0:
            warn(f"Some outflows data left unused: {unused_outflows_data}", Warning)
        if len(unused_total_population_data) > 0:
            warn(f"Some total population data left unused: {unused_total_population_data}", Warning)
