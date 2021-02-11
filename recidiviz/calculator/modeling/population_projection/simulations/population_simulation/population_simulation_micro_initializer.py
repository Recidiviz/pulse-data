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

from typing import Dict, List, Any
from functools import partial
import pandas as pd

from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.\
    population_simulation_initializer import PopulationSimulationInitializer
from recidiviz.calculator.modeling.population_projection.simulations.compartment_transitions import \
    CompartmentTransitions
from recidiviz.calculator.modeling.population_projection.simulations.sub_simulation.sub_simulation import SubSimulation
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class PopulationSimulationMicroInitializer(PopulationSimulationInitializer):
    """Handles set-up specific logic of PopulationSimulation for micro-sim."""

    def initialize_simulation(self, outflows_data: pd.DataFrame, transitions_data: pd.DataFrame,
                              total_population_data: pd.DataFrame,
                              simulation_compartments: Dict[str, str],
                              disaggregation_axes: List[str], user_inputs: Dict, first_relevant_ts: int,
                              sub_group_ids_dict: Dict[str, Dict[str, Any]],
                              microsim_data: pd.DataFrame,
                              sub_simulations: Dict[str, SubSimulation]) -> None:
        """Initialize the simulation parameters along with all of the sub simulations for microsim"""
        if len(user_inputs['policy_list']) != 0:
            raise ValueError("Microsim option does not support policy inputs")

        super()._populate_sub_group_ids_dict(transitions_data, disaggregation_axes, sub_group_ids_dict)

        # populate "policy list" to switch from remaining sentences data to transitions data
        for sub_group_id, group_attributes in sub_group_ids_dict.items():
            disaggregated_microsim_data = \
                microsim_data[(microsim_data[disaggregation_axes] == pd.Series(group_attributes)).all(axis=1)]

            # add one policy per compartment to switch transitions data from remaining sentence data to transitions data
            for full_comp in [i for i in simulation_compartments
                              if simulation_compartments[i] != 'shell']:
                user_inputs['policy_list'].append(SparkPolicy(
                    policy_fn=partial(
                        CompartmentTransitions.use_alternate_transitions_data,
                        alternate_historical_transitions=
                        disaggregated_microsim_data[disaggregated_microsim_data.compartment == full_comp],
                        retroactive=False
                    ),
                    spark_compartment=full_comp,
                    sub_population=sub_group_ids_dict[sub_group_id],
                    apply_retroactive=False
                ))

        self._populate_sub_simulations(outflows_data, transitions_data, total_population_data, simulation_compartments,
                                       disaggregation_axes, user_inputs, first_relevant_ts, sub_group_ids_dict,
                                       sub_simulations)
