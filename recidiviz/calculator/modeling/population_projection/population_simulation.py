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

from typing import Dict, List, Optional
from time import time
from functools import partial
import pandas as pd

from recidiviz.calculator.modeling.population_projection.compartment_transitions import CompartmentTransitions
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.sub_simulation import SubSimulation


class PopulationSimulation:
    """Control the many sub simulations for one scenario (baseline/control or policy)"""
    def __init__(self):
        self.sub_simulations = {}
        self.sub_group_ids_dict = {}
        self.validation_population_data = None
        self.validation_transition_data = None
        self.population_projections = None

    def simulate_policies(self, outflows_data: pd.DataFrame,
                          transitions_data: pd.DataFrame,
                          total_population_data: pd.DataFrame,
                          simulation_compartments: Dict[str, Optional[CompartmentTransitions]],
                          disaggregation_axes: List[str],
                          user_inputs: Dict,
                          initialization_period=2,
                          microsim: bool = False,
                          microsim_data: pd.DataFrame = None):
        """
        Run a population projection and return population counts by year, compartment, and sub-group.
        `outflows_data` should be a DataFrame with columns for each sub-group, year, compartment, outflow_to,
            and total_population
        `transitions_data` should be a DataFrame with columns for each dis-aggregation axis, compartment,
            outflow_to, compartment_duration, and total_population
        `total_population_data` should be a DataFrame with columns for each disaggregation axis, compartment,
            time_step and total_population
        `disaggregation_axes` should be names of columns in historical_data that refer to sub-group categorizations
        `user_inputs` should be a dict including:
            'projection_time_steps': number of ts to project forward
            'policy_time_step': ts policies take effect
            'start_time_step': first ts of projection
            'policy_list': List of SparkPolicy objects to use for this simulation
            'backcast_projection': True if the simulation should be initialized with a backcast projection
            'constant_admissions': True if the admission population total should remain constant for each time step
        `initialization_period` should be a number representing how many times max_sentence to go backward for
            initialization. Default is 2 to ensure long-sentence cohorts are well-populated.
        `microsim` should be a boolean indicating whether the simulation is a micro-simulation
        `microsim_data` should only be passed if microsim, and should be a DataFrame with real transitions data, whereas
            in that case `transitions_data` will actually be remaining_duration_data
        """

        start = time()

        self.validation_population_data = total_population_data

        # Check the inputs have the required fields and types
        required_user_inputs = ['projection_time_steps', 'policy_time_step', 'start_time_step', 'policy_list',
                                'constant_admissions', 'speed_run']
        missing_inputs = [key for key in required_user_inputs if key not in user_inputs]
        if len(missing_inputs) != 0:
            raise ValueError(f"Required user input are missing: {missing_inputs}")

        if any(not isinstance(policy, SparkPolicy) for policy in user_inputs['policy_list']):
            raise ValueError(f"Policy list can only include SparkPolicy objects: {user_inputs['policy_list']}")

        for axis in disaggregation_axes:
            for df in [outflows_data, transitions_data, total_population_data]:
                if axis not in df.columns:
                    raise ValueError(f"All disagregation axis must be included in the input dataframe columns\n"
                                     f"Expected: {disaggregation_axes}, Actual: {df.columns}")

        # use outflows data to compute the transitions if the transitions_data df is not provided
        if transitions_data is None:
            transitions_data = outflows_data

        self.initialize_simulation(outflows_data, transitions_data, total_population_data, simulation_compartments,
                                   disaggregation_axes, user_inputs, initialization_period, microsim, microsim_data)

        time1 = time()
        print('initialization time: ', time1 - start)

        # Run the sub simulations for each ts
        self.step_forward(user_inputs['projection_time_steps'])

        #  Store the results in one Dataframe
        for simulation_group_id, simulation_obj in self.sub_simulations.items():
            sub_population_projection = simulation_obj.get_population_projections()
            sub_population_projection['simulation_group'] = simulation_group_id
            self.population_projections = pd.concat([self.population_projections, sub_population_projection])

        time2 = time()

        print('simulation_time: ', time2 - time1)

        return self.population_projections

    def initialize_simulation(self, outflows_data: pd.DataFrame,
                              transitions_data: pd.DataFrame,
                              total_population_data: pd.DataFrame,
                              simulation_compartments: Dict[str, Optional[CompartmentTransitions]],
                              disaggregation_axes: List[str],
                              user_inputs: Dict,
                              initialization_period=2,
                              microsim: bool = False,
                              microsim_data: pd.DataFrame = None):
        """Initialize the simulation parameters along with all of the sub simulations"""

        #  defined in both cases because it's required as an input to sub-simulation, but not actually
        #   used in microsimulations
        first_relevant_ts = None
        if not microsim:
            # TODO(#4512): cap this at 50 years (non-trivial because ts unit unknown)
            if user_inputs['speed_run']:
                max_sentence = user_inputs['projection_time_steps'] + 1
            else:
                max_sentence = max(transitions_data.compartment_duration)
            first_relevant_ts = int(user_inputs['start_time_step'] - initialization_period * max_sentence)

        self.population_projections = pd.DataFrame()

        # populate self.sub_group_ids_dict so we can recover sub-group properties during validation
        for simulation_group_name, _ in transitions_data.groupby(disaggregation_axes):
            sub_group_id = str(simulation_group_name)

            if len(disaggregation_axes) == 1:
                self.sub_group_ids_dict[sub_group_id] = {disaggregation_axes[0]: simulation_group_name}
            else:
                self.sub_group_ids_dict[sub_group_id] = \
                    {disaggregation_axes[i]: simulation_group_name[i] for i in range(len(disaggregation_axes))}

        # Initialize one sub simulation per sub-population
        for sub_group_id in self.sub_group_ids_dict:
            group_attributes = pd.Series(self.sub_group_ids_dict[sub_group_id])
            disaggregated_transitions_data = \
                transitions_data[(transitions_data[disaggregation_axes] == group_attributes).all(axis=1)]
            disaggregated_outflows_data = \
                outflows_data[(outflows_data[disaggregation_axes] == group_attributes).all(axis=1)]
            disaggregated_total_population_data = \
                total_population_data[(total_population_data[disaggregation_axes] == group_attributes).all(axis=1)]

            # Select the policies relevant to this simulation group

            # if micro-simulation, "policy" should be subbing in proper transitions data for the new admissions
            if microsim:
                if microsim_data is None:
                    raise ValueError("Microsim data is required for microsim mode")
                if len(user_inputs['policy_list']) != 0:
                    raise ValueError("Microsim option does not support policy inputs")
                disaggregated_microsim_data = \
                    microsim_data[(microsim_data[disaggregation_axes] == group_attributes).all(axis=1)]
                group_policies = list()
                for full_comp in [i for i in simulation_compartments
                                  if simulation_compartments[i] is not None]:
                    group_policies.append(SparkPolicy(
                        policy_fn=partial(
                            CompartmentTransitions.use_alternate_transitions_data,
                            alternate_historical_transitions=
                            disaggregated_microsim_data[disaggregated_microsim_data.compartment == full_comp],
                            retroactive=False
                        ),
                        spark_compartment=full_comp,
                        sub_population=self.sub_group_ids_dict[sub_group_id],
                        apply_retroactive=False
                    ))

            else:
                group_policies = SparkPolicy.get_sub_population_policies(user_inputs['policy_list'],
                                                                         self.sub_group_ids_dict[sub_group_id])
            self.sub_simulations[sub_group_id] = SubSimulation(
                outflows_data=disaggregated_outflows_data,
                transitions_data=disaggregated_transitions_data,
                total_population_data=disaggregated_total_population_data,
                simulation_compartments=simulation_compartments,
                user_inputs=user_inputs,
                policy_list=group_policies,
                first_relevant_ts=first_relevant_ts,
                microsim=microsim
            )
            self.sub_simulations[sub_group_id].initialize()

        if not microsim:
            # run simulation up to the start_year if the back-cast option is enabled
            self.step_forward(user_inputs['start_time_step'] - first_relevant_ts)
            for simulation_obj in self.sub_simulations.values():
                simulation_obj.scale_total_populations()

    def step_forward(self, num_ts: int):
        for _ in range(num_ts):
            for simulation_obj in self.sub_simulations.values():
                simulation_obj.step_forward()

            cross_simulation_flows = pd.DataFrame(columns=['sub_group_id', 'compartment'])
            for sub_group_id, simulation_obj in self.sub_simulations.items():
                simulation_cohorts = simulation_obj.cross_flow()
                simulation_cohorts['sub_group_id'] = sub_group_id
                cross_simulation_flows = pd.concat([cross_simulation_flows, simulation_cohorts], sort=True)

            unassigned_cohorts = cross_simulation_flows[cross_simulation_flows.compartment.isnull()]
            if len(unassigned_cohorts) > 0:
                raise ValueError(f'cohorts passed up without compartment: {unassigned_cohorts}')

            cross_simulation_flows = self.update_cohort_attributes(cross_simulation_flows)

            for sub_group_id, simulation_obj in self.sub_simulations.items():
                sub_group_cohorts = cross_simulation_flows[cross_simulation_flows.sub_group_id
                                                           == sub_group_id].drop('sub_group_id', axis=1)
                simulation_obj.ingest_cross_simulation_cohorts(sub_group_cohorts)

    @staticmethod
    def update_cohort_attributes(cross_simulation_flows):
        """Should change sub_group_id for each row to whatever simulation that cohort should move to in the next ts"""
        return cross_simulation_flows

    def calculate_transition_error(self, validation_data: pd.DataFrame):
        """
        validation_data should be a DataFrame with exactly:
            one column per axis of disaggregation, 'time_step', 'count', 'compartment', 'outflow_to'
        """

        self.validation_transition_data = validation_data

        aggregated_results = self.validation_transition_data.copy()
        aggregated_results['count'] = 0

        for sub_group_id, sub_group_obj in self.sub_simulations.items():
            for compartment_tag, compartment in sub_group_obj.simulation_compartments.items():
                for ts in set(self.validation_transition_data['time_step']):
                    index_locator = aggregated_results[aggregated_results['time_step'] == ts]
                    sub_group_id_dict = self.sub_group_ids_dict[sub_group_id]
                    sub_group_id_dict['compartment'] = compartment_tag
                    for axis in sub_group_id_dict:
                        if axis in index_locator.columns:
                            keepers = [sub_group_id_dict[axis] in i for i in index_locator[axis]]
                            index_locator = index_locator[keepers]

                    validation_indices = {outflow: index for index, outflow in
                                          index_locator['outflow_to'].iteritems()}

                    for outflow in validation_indices:
                        aggregated_results.loc[validation_indices[outflow], 'count'] += \
                            compartment.outflows[ts][outflow]
        return aggregated_results

    def gen_arima_output_df(self):
        arima_output_df = pd.DataFrame()
        for simulation_group, sub_simulation in self.sub_simulations.items():
            output_df_sub = \
                pd.concat({simulation_group: sub_simulation.gen_arima_output_df()}, names=['simulation_group'])
            arima_output_df = arima_output_df.append(output_df_sub)
        return arima_output_df

    def gen_scale_factors_df(self):
        scale_factors_df = pd.DataFrame(columns=self.sub_group_ids_dict.keys())
        for subgroup_name, subgroup_obj in self.sub_simulations.items():
            if subgroup_name not in scale_factors_df.columns:
                raise ValueError(f"{subgroup_name} sub simulation was not initialized in sub_group_ids_dict")
            scale_factors_df[subgroup_name] = subgroup_obj.get_scale_factors()
        return scale_factors_df

    def gen_total_population_error(self):
        total_population_error = pd.DataFrame(index=self.validation_population_data.time_step.unique(),
                                              columns=self.validation_population_data.compartment.unique())

        min_projection_ts = min(self.population_projections['time_step'])
        for compartment in total_population_error.columns:
            for ts in total_population_error.index:
                if ts < min_projection_ts:
                    continue
                simulation_population = \
                    self.population_projections[(self.population_projections.compartment == compartment) &
                                                (self.population_projections.time_step == ts)].total_population.sum()
                historical_population = self.validation_population_data[
                        (self.validation_population_data.compartment == compartment) &
                        (self.validation_population_data.time_step == ts)].total_population.sum()

                if simulation_population == 0:
                    raise ValueError(f"Simulation population total for compartment {compartment} and time step {ts} "
                                     "cannot be 0 for validation")
                if historical_population == 0:
                    raise ValueError(f"Historical population data for compartment {compartment} and time step {ts} "
                                     "cannot be 0 for validation")

                total_population_error.loc[ts, compartment] = \
                    (simulation_population - historical_population) / historical_population

        return total_population_error.sort_index()
