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
"""Highest level simulation object -- runs various comparative scenarios"""

from typing import Dict, List, Tuple, Any
from copy import deepcopy
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation import SuperSimulation
from recidiviz.calculator.modeling.population_projection.population_simulation import PopulationSimulation
from recidiviz.calculator.modeling.population_projection import spark_bq_utils
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class MacroSuperSimulation(SuperSimulation):
    """Manage the PopulationSimulations and output data needed to run tests, baselines, and policy scenarios"""

    def _initialize_data(self, data_inputs_params: Dict[str, Any]):
        """Initialize the data_dict from Big Query"""
        simulation_tag = data_inputs_params['big_query_simulation_tag']
        input_data_tables = {
                'outflows_data': spark_bq_utils.OUTFLOWS_DATA_TABLE_NAME,
                'transitions_data': spark_bq_utils.TRANSITIONS_DATA_TABLE_NAME,
                'total_population_data': spark_bq_utils.TOTAL_POPULATION_DATA_TABLE_NAME
        }

        for table_tag, table_bq_name in input_data_tables.items():
            table_data = spark_bq_utils.load_spark_table_from_big_query(table_bq_name, simulation_tag)
            print(f"{table_tag} returned {len(table_data)} results")
            self.data_dict[table_tag] = table_data

    def _initialize_data_csv(self, initialization_params: Dict[str, Any]):
        """Initialize the data_dict from CSV data --> deprecated"""
        simulation_data = pd.read_csv(initialization_params['state_data'])

        transitions_data = simulation_data[simulation_data.compartment_duration.notnull()]
        outflows_data = simulation_data[(simulation_data.compartment_duration.isnull()) &
                                        (simulation_data.outflow_to.notnull())]
        total_population_data = simulation_data[simulation_data.outflow_to.isnull()]

        null_ts_outflows = outflows_data[outflows_data.time_step.isnull()]
        if len(null_ts_outflows) != 0:
            raise ValueError(f"Outflows data contains null time steps: {null_ts_outflows}")

        null_ts_total_population = total_population_data[total_population_data.time_step.isnull()]
        if len(null_ts_total_population) != 0:
            raise ValueError(f"Total population data contains null time steps: {null_ts_total_population}")

        if any(outflows_data['time_step'] != outflows_data['time_step'].apply(int)):
            raise ValueError(f"Outflows data time steps cannot be converted to ints: {outflows_data['time_step']}")
        outflows_data.loc[outflows_data.index, 'time_step'] = outflows_data['time_step'].apply(int)

        if any(total_population_data['time_step'] != total_population_data['time_step'].apply(int)):
            raise ValueError(f"Total population time steps cannot be converted to ints: {outflows_data['time_step']}")
        total_population_data.loc[total_population_data.index, 'time_step'] = \
            total_population_data['time_step'].apply(int)

        self.data_dict['outflows_data'] = outflows_data
        self.data_dict['transitions_data'] = transitions_data
        self.data_dict['total_population_data'] = total_population_data

    def _set_user_inputs(self, yaml_user_inputs: Dict[str, Any]):
        super()._set_user_inputs(yaml_user_inputs)
        self.user_inputs['policy_time_step'] = \
            self._convert_to_relative_date(yaml_user_inputs['policy_year'])
        if 'policy_list' not in yaml_user_inputs:
            self.user_inputs['policy_list'] = []
        else:
            self.user_inputs['policy_list'] = yaml_user_inputs['policy_list']

    def _get_first_relevant_ts(self):
        """calculate ts to start model initialization at"""
        # TODO(#4512): cap this at 50 years (non-trivial because ts unit unknown)
        if self.user_inputs['speed_run']:
            max_sentence = self.user_inputs['projection_time_steps'] + 1
        else:
            max_sentence = max(self.data_dict['transitions_data'].compartment_duration)
        return int(self.user_inputs['start_time_step'] - 2 * max_sentence)

    def simulate_policy(self, policy_list: List[SparkPolicy], output_compartment: str):
        """
        Run one PopulationSimulation with policy implemented and one baseline, returns cumulative and non-cumulative
            life-years diff, cost diff, total pop diff, all by compartment
        `policy_list` should be a list of SparkPolicy objects to be applied in the policy scenario
        `output_compartment` should be the primary compartment to be graphed at the end (doesn't affect calculation)
        """
        # TODO(#4870): update old functions
        self._reset_pop_simulations()

        self.user_inputs['policy_list'] = policy_list

        self.pop_simulations['policy'] = PopulationSimulation()
        self.pop_simulations['control'] = PopulationSimulation()

        simulation_data_inputs = (
            self.data_dict['outflows_data'],
            self.data_dict['transitions_data'],
            self.data_dict['total_population_data'],
            self.data_dict['simulation_compartments_architecture'],
            self.data_dict['disaggregation_axes']
        )

        first_relevant_ts = self._get_first_relevant_ts()

        self.pop_simulations['policy'].simulate_policies(*simulation_data_inputs, user_inputs=self.user_inputs,
                                                         first_relevant_ts=first_relevant_ts)

        control_user_inputs = deepcopy(self.user_inputs)
        control_user_inputs['policy_list'] = []
        self.pop_simulations['control'].simulate_policies(*simulation_data_inputs, user_inputs=control_user_inputs,
                                                          first_relevant_ts=first_relevant_ts)
        results = {scenario: self.pop_simulations[scenario].population_projections for scenario in self.pop_simulations}
        results = {i: results[i][results[i]['time_step'] >= self.user_inputs['start_time_step']] for i in results}
        self.output_data['policy_simulation'] = self._graph_results(results, output_compartment)

        return self._get_output_metrics()

    def _graph_results(self, simulations: Dict[str, pd.DataFrame], output_compartment: str):
        simulation_keys = list(simulations.keys())
        simulation_results = self._format_simulation_results(simulations[simulation_keys[0]], simulation_keys[0])
        for simulation_name in simulation_keys[1:]:
            formatted_result = self._format_simulation_results(simulations[simulation_name], simulation_name)
            simulation_results = simulation_results.merge(formatted_result, on=['compartment', 'year'])

        simulation_results[simulation_results['compartment'] == output_compartment].plot(
            x='year', y=[f'{simulation_name}_total_population' for simulation_name in simulations.keys()])
        plt.title(f"Policy Impact on {output_compartment} Population")
        plt.ylabel(f"Estimated Year End {output_compartment} Population")
        plt.legend(loc='lower left')
        plt.ylim([0, None])

        return simulation_results

    def _get_output_metrics(self):
        """
        Generates savings and life-years saved; helper function for simulate_policy()
        `output_compartment` should be the compartment for which to calculate life-years saved
        """
        projection = self.output_data['policy_simulation'].copy().set_index('year').sort_values('year')
        compartment_life_years_diff = pd.DataFrame(0, index=projection.index.unique(),
                                                   columns=projection.compartment.unique())

        for compartment_name, compartment_data in projection.groupby('compartment'):
            compartment_life_years_diff.loc[compartment_data.index, compartment_name] = \
                (compartment_data['control_total_population']
                 - compartment_data['policy_total_population']) * self.time_step

        spending_diff_non_cumulative = compartment_life_years_diff.copy()
        compartment_life_years_diff = compartment_life_years_diff.cumsum()

        spending_diff = compartment_life_years_diff.copy()

        for compartment in self.compartment_costs:
            spending_diff[compartment] *= self.compartment_costs[compartment]
            spending_diff_non_cumulative[compartment] *= self.compartment_costs[compartment]

        # Store output metrics in the output_data dict
        self.output_data['cost_avoidance'] = spending_diff
        self.output_data['life_years'] = compartment_life_years_diff
        self.output_data['cost_avoidance_non_cumulative'] = spending_diff_non_cumulative

        return spending_diff, compartment_life_years_diff, spending_diff_non_cumulative

    def _simulate_baseline(self, simulation_title: str, first_relevant_ts: int = None):
        """
        Calculates a baseline projection, returns transition error for a specific transition
        `simulation_title` is the desired tag of the PopulationSimulation
        `initialization_period` is optional paramter for number of years to go backward for initialization in units of
            the max length of transition tables (e.g. 2 = go back twice the max_sentence)
        """

        super()._simulate_baseline(simulation_title)

        simulation_data_inputs = (
            self.data_dict['outflows_data'],
            self.data_dict['transitions_data'],
            self.data_dict['total_population_data'],
            self.data_dict['simulation_compartments_architecture'],
            self.data_dict['disaggregation_axes']
        )

        if first_relevant_ts is None:
            first_relevant_ts = self._get_first_relevant_ts()

        self.pop_simulations[simulation_title].simulate_policies(*simulation_data_inputs, self.user_inputs,
                                                                 first_relevant_ts)

        self.output_data[simulation_title] = \
            self.pop_simulations[simulation_title].population_projections.sort_values('time_step')

    def calculate_cohort_population_error(self, output_compartment: str, outflow_to: str,
                                          back_fill_range: tuple = (0, 2, 0.1), unit: str = 'abs'):
        """
        `backfill_range` is a three item tuple giving the lower and upper bounds to test in units of
            subgroup max_sentence and the step size
        `output_compartment` is the compartment whose error you want to get
        `outflow_to` is the outflow from that compartment you want to get the error on
        `unit is either mse or abs`
        TODO(#4870): update old functions
        """
        self._reset_pop_simulations()

        range_start, range_end, step_size = back_fill_range

        for ts in np.arange(range_start, range_end, step_size):
            self.pop_simulations[f"backfill_period_{ts}_max_sentences"] = PopulationSimulation()
            self.pop_simulations[f"backfill_period_{ts}_max_sentences"].simulate_policies(
                self.data_dict['outflows_data'],
                self.data_dict['transitions_data'],
                self.data_dict['total_population_data'],
                self.data_dict['simulation_compartments_architecture'],
                self.data_dict['disaggregation_axes'],
                self.user_inputs,
                ts
            )

        self.output_data['cohort_population_error'] = pd.DataFrame()
        for test_sim in self.pop_simulations:
            errors = pd.Series(dtype=float)
            for sub_group in self.pop_simulations[test_sim].sub_simulations:
                sub_group_sim = self.pop_simulations[test_sim].sub_simulations[sub_group]
                errors[sub_group] = sub_group_sim.get_error(output_compartment, unit=unit)[outflow_to].abs().mean()
            self.output_data['cohort_population_error'][test_sim] = errors

        self.output_data['cohort_population_error'] = self.output_data['cohort_population_error'].transpose()
        self.output_data['cohort_population_error'].index = np.arange(range_start, range_end, step_size)
        # skip first step because can't calculate ts-over-ts error differential from previous ts
        error_differential = pd.DataFrame(
            index=np.arange(range_start + step_size, range_end, step_size),
            columns=self.output_data['cohort_population_error'].columns
        )
        # compute the ts-over-ts error differential
        for ts in range(len(error_differential.index)):
            for sub_group in error_differential.columns:
                error_differential.iloc[ts][sub_group] = \
                    self.output_data['cohort_population_error'].iloc[ts + 1][sub_group] - \
                    self.output_data['cohort_population_error'].iloc[ts][sub_group]

        plt.plot(error_differential)
        plt.ylabel(f'time_step-over-time_step differential in {unit}')
        plt.xlabel('number of max_sentences of back-filling')
        plt.title(f'error in releases from {output_compartment}')

        return self.output_data['cohort_population_error']

    def calculate_outflows_data_sparsity_error(self, output_compartment: str, outflow_to: str,
                                               ts_to_keep: Tuple[int, int] = (1, 11), unit: str = 'abs'):
        """
        `ts_to_keep` is a two item list giving the lower and upper bounds of number of ts of data to keep.
            Lower bound cannot be less than 1
        `output_compartment` is the compartment whose error you want to get
        `outflow_to` is the outflow from that compartment you want to get the error on
        `unit is either mse or abs`
        TODO(#4870): update old functions
        """
        self._reset_pop_simulations()

        for ts in range(ts_to_keep[0], ts_to_keep[1]):
            new_data_start_year = max(self.data_dict['outflows_data']['time_step']) - ts
            self.pop_simulations[f"with_{ts}_time_steps_of_historical_data"] = PopulationSimulation()
            self.pop_simulations[f"with_{ts}_time_steps_of_historical_data"].simulate_policies(
                self.data_dict['outflows_data'][self.data_dict['outflows_data']['time_step'] > new_data_start_year],
                self.data_dict['transitions_data'],
                self.data_dict['total_population_data'],
                self.data_dict['simulation_compartments_architecture'],
                self.data_dict['disaggregation_axes'],
                self.user_inputs,
                0
            )

        self.output_data['release_data_sparsity_error'] = pd.DataFrame()
        for test_sim in self.pop_simulations:
            errors = pd.Series()
            for sub_group in self.pop_simulations[test_sim].sub_simulations:
                sub_group_sim = self.pop_simulations[test_sim].sub_simulations[sub_group]
                errors[sub_group] = sub_group_sim.get_error(output_compartment, unit=unit)[outflow_to].abs().mean()
            self.output_data[f'{outflow_to}_data_sparsity_error'][test_sim] = errors

        self.output_data[f'{outflow_to}_data_sparsity_error'] = \
            self.output_data[f'{outflow_to}_data_sparsity_error'].transpose()
        self.output_data[f'{outflow_to}_data_sparsity_error'].index = range(ts_to_keep[0], ts_to_keep[1])
        error_differential = pd.DataFrame(index=range(ts_to_keep[0] + 1, ts_to_keep[1]),
                                          columns=self.output_data[f'{outflow_to}_data_sparsity_error'].columns)
        for ts in error_differential.index:
            for sub_group in error_differential.columns:
                error_differential.loc[ts, sub_group] = \
                    self.output_data[f'{outflow_to}_data_sparsity_error'].loc[ts, sub_group] - \
                    self.output_data[f'{outflow_to}_data_sparsity_error'].loc[ts - 1, sub_group]

        plt.plot(error_differential)
        plt.ylabel(f'time_step-over-time_step differential in {unit}')
        plt.xlabel('number of time_steps of historical data')
        plt.title(f'error in releases from {output_compartment}')

        return self.output_data[f'{outflow_to}_data_sparsity_error']

    def upload_simulation_results_to_bq(self, project_id, simulation_tag):
        required_keys = ['policy_simulation', 'cost_avoidance', 'life_years', 'cost_avoidance_non_cumulative']
        missing_keys = [key for key in required_keys if key not in self.output_data.keys()]
        if len(missing_keys) != 0:
            raise ValueError(f"Output data is missing the required columns {missing_keys}")
        spark_bq_utils.upload_spark_results(project_id, simulation_tag, self.output_data['cost_avoidance'],
                                            self.output_data['life_years'], self.output_data['policy_simulation'],
                                            self.output_data['cost_avoidance_non_cumulative'])
