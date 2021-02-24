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

from typing import Dict, List, Tuple, Any, Optional
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation import SuperSimulation
from recidiviz.calculator.modeling.population_projection import spark_bq_utils
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.population_simulation \
    import PopulationSimulation
from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.\
    population_simulation_factory import PopulationSimulationFactory


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

    def _set_user_inputs(self, yaml_user_inputs: Dict[str, Any]):
        super()._set_user_inputs(yaml_user_inputs)
        self.user_inputs['policy_time_step'] = \
            self._convert_to_relative_date(yaml_user_inputs['policy_year'])
        self.user_inputs['policy_list'] = []

    def _build_population_simulation(self,
                                     first_relevant_ts_override: Optional[int] = None,
                                     outflows_data_override: Optional[pd.DataFrame] = None,
                                     user_inputs: Dict[str, Any] = None
                                     ) -> PopulationSimulation:
        if user_inputs is None:
            user_inputs = self.user_inputs

        if first_relevant_ts_override is None:
            first_relevant_ts_override = self._get_first_relevant_ts()

        outflows_data_override = outflows_data_override or self.data_dict['outflows_data']

        return PopulationSimulationFactory.build_population_simulation(
            outflows_data=outflows_data_override,
            transitions_data=self.data_dict['transitions_data'],
            total_population_data=self.data_dict['total_population_data'],
            simulation_compartments=self.data_dict['compartments_architecture'],
            disaggregation_axes=self.data_dict['disaggregation_axes'],
            user_inputs=user_inputs,
            first_relevant_ts=first_relevant_ts_override,
            microsim_data=pd.DataFrame(),
            should_initialize_compartment_populations=False,
            should_scale_populations_after_step=True
        )

    def simulate_policy(self, policy_list: List[SparkPolicy], output_compartment: str,
                        cost_multipliers: Optional[pd.DataFrame] = None):
        """
        Run one PopulationSimulation with policy implemented and one baseline, returns cumulative and non-cumulative
            life-years diff, cost diff, total pop diff, all by compartment
        `policy_list` should be a list of SparkPolicy objects to be applied in the policy scenario
        `output_compartment` should be the primary compartment to be graphed at the end (doesn't affect calculation)
        `cost_multipliers` should be a df with one column per disaggregation axis and a column `multiplier`
        """
        self._reset_pop_simulations()

        policy_user_inputs = self.user_inputs.copy()
        policy_user_inputs['policy_list'] = policy_list

        self.pop_simulations['policy'] = self._build_population_simulation(user_inputs=policy_user_inputs)
        self.pop_simulations['control'] = self._build_population_simulation()

        self.pop_simulations['policy'].simulate_policies()
        self.pop_simulations['control'].simulate_policies()

        results = {scenario: self.pop_simulations[scenario].population_projections for scenario in self.pop_simulations}
        results = {i: results[i][results[i]['time_step'] >= self.user_inputs['start_time_step']] for i in results}
        self.output_data['policy_simulation'] = self._graph_results(results, output_compartment)

        if cost_multipliers is None:
            cost_multipliers = pd.DataFrame(columns=self.data_dict['disaggregation_axes'] + ['multiplier'])

        missing_disaggregation_axes = [axis for axis in self.data_dict['disaggregation_axes']
                                       if axis not in cost_multipliers]
        if len(missing_disaggregation_axes) > 0:
            raise ValueError(f"Cost multipliers df missing disaggregation axes: {missing_disaggregation_axes}")

        # fill in missing subgroups with identity multiplier = 1
        for subgroup_dict in self.pop_simulations['control'].sub_group_ids_dict.values():
            if cost_multipliers[(cost_multipliers[self.data_dict['disaggregation_axes']] ==
                                pd.Series(subgroup_dict)).all(axis=1)].empty:
                cost_multipliers = cost_multipliers.append({**subgroup_dict, **{'multiplier': 1}}, ignore_index=True)

        return self._get_output_metrics(cost_multipliers)

    def _graph_results(self, simulations: Dict[str, pd.DataFrame], output_compartment: str):
        simulation_results = self._format_simulation_results(collapse_compartments=True)

        simulation_results[simulation_results['compartment'] == output_compartment].plot(
            x='year', y=[f'{simulation_name}_total_population' for simulation_name in simulations.keys()])
        plt.title(f"Policy Impact on {output_compartment} Population")
        plt.ylabel(f"Estimated Year End {output_compartment} Population")
        plt.legend(loc='lower left')
        plt.ylim([0, None])

        return simulation_results

    def _get_output_metrics(self, cost_multipliers: pd.DataFrame):
        """
        Generates savings and life-years saved; helper function for simulate_policy()
        `cost_multipliers` should be a df of how to scale the per_year_cost for each subgroup
        """
        simulation_results = self._format_simulation_results()

        compartment_life_years_diff = pd.DataFrame()
        spending_diff_non_cumulative = pd.DataFrame()
        spending_diff = pd.DataFrame()

        # go through and calculate differences for each subgroup
        for subgroup_tag, subgroup_dict in self.pop_simulations['control'].sub_group_ids_dict.items():
            subgroup_data = simulation_results[
                simulation_results['simulation_group'] == subgroup_tag]

            subgroup_life_years_diff = pd.DataFrame(index=subgroup_data.year.unique(),
                                                    columns=subgroup_data.compartment.unique())

            for compartment_name, compartment_data in subgroup_data.groupby('compartment'):
                subgroup_life_years_diff.loc[compartment_data.year, compartment_name] = \
                    (compartment_data['control_total_population']
                     - compartment_data['policy_total_population']) * self.time_step

            subgroup_spending_diff_non_cumulative = subgroup_life_years_diff.copy() / self.time_step
            subgroup_life_years_diff = subgroup_life_years_diff.cumsum()
            subgroup_spending_diff = subgroup_life_years_diff.copy()

            # pull out cost multiplier for this subgroup
            multiplier = cost_multipliers[(cost_multipliers[self.data_dict['disaggregation_axes']] ==
                                          pd.Series(subgroup_dict)).all(axis=1)].iloc[0].multiplier

            for compartment in self.compartment_costs:
                subgroup_spending_diff[compartment] *= self.compartment_costs[compartment] * multiplier
                subgroup_spending_diff_non_cumulative[compartment] *= self.compartment_costs[compartment] * multiplier

            # add subgroup outputs to total outputs
            compartment_life_years_diff = compartment_life_years_diff.add(subgroup_life_years_diff, fill_value=0)
            spending_diff_non_cumulative = spending_diff_non_cumulative.add(subgroup_spending_diff_non_cumulative,
                                                                            fill_value=0)
            spending_diff = spending_diff.add(subgroup_spending_diff, fill_value=0)

        spending_diff.index.name = 'year'
        compartment_life_years_diff.index.name = 'year'
        spending_diff_non_cumulative.index.name = 'year'

        # Store output metrics in the output_data dict
        self.output_data['cost_avoidance'] = spending_diff
        self.output_data['life_years'] = compartment_life_years_diff
        self.output_data['cost_avoidance_non_cumulative'] = spending_diff_non_cumulative

        return spending_diff, compartment_life_years_diff, spending_diff_non_cumulative

    def calculate_cohort_hydration_error(self, output_compartment: str, outflow_to: str,
                                         back_fill_range: tuple = (0, 2, 0.1), unit: str = 'abs'):
        """
        `backfill_range` is a three item tuple giving the lower and upper bounds to test in units of
            subgroup max_sentence and the step size
        `output_compartment` is the compartment whose error you want to get
        `outflow_to` is the outflow from that compartment you want to get the error on
        `unit is either mse or abs`
        """
        self._reset_pop_simulations()

        max_sentence = self.user_inputs['start_time_step'] - self._get_first_relevant_ts()
        range_start, range_end, step_size = [int(i * max_sentence) for i in back_fill_range]

        for ts in np.arange(range_start, range_end, step_size):
            self.pop_simulations[f"backfill_period_{ts}_time_steps"] = \
                self._build_population_simulation(first_relevant_ts_override=self.user_inputs['start_time_step'] - ts)
            self.pop_simulations[f"backfill_period_{ts}_time_steps"].simulate_policies()

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

        error_differential.plot(ylabel=f'time_step-over-time_step differential in {unit}',
                                xlabel='number of max_sentences of back-filling',
                                title=f'error in releases from {output_compartment}')

        return self.output_data['cohort_population_error']

    def calculate_outflows_data_sparsity_error(self, output_compartment: str, outflow_to: str,
                                               ts_to_keep: Tuple[int, int] = (1, 11), unit: str = 'abs'):
        """
        `ts_to_keep` is a two item list giving the lower and upper bounds of number of ts of data to keep.
            Lower bound cannot be less than 1
        `output_compartment` is the compartment whose error you want to get
        `outflow_to` is the outflow from that compartment you want to get the error on
        `unit is either mse or abs`
        """
        self._reset_pop_simulations()

        for ts in range(ts_to_keep[0], ts_to_keep[1]):
            new_data_start_year = max(self.data_dict['outflows_data']['time_step']) - ts
            outflows = \
                self.data_dict['outflows_data'][self.data_dict['outflows_data']['time_step'] > new_data_start_year]
            self.pop_simulations[f"with_{ts}_time_steps_of_historical_data"] = \
                self._build_population_simulation(outflows_data_override=outflows)
            self.pop_simulations[f"with_{ts}_time_steps_of_historical_data"].simulate_policies()

        self.output_data[f'{outflow_to}_data_sparsity_error'] = pd.DataFrame()
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

        error_differential.plot(ylabel=f'time_step-over-time_step differential in {unit}',
                                xlabel='number of time_steps of historical data',
                                title=f'error in releases from {output_compartment}')

        return self.output_data[f'{outflow_to}_data_sparsity_error']

    def upload_simulation_results_to_bq(self, project_id, simulation_tag):
        required_keys = ['policy_simulation', 'cost_avoidance', 'life_years', 'cost_avoidance_non_cumulative']
        missing_keys = [key for key in required_keys if key not in self.output_data.keys()]
        if len(missing_keys) != 0:
            raise ValueError(f"Output data is missing the required columns {missing_keys}")
        spark_bq_utils.upload_spark_results(project_id, simulation_tag, self.output_data['cost_avoidance'],
                                            self.output_data['life_years'], self.output_data['policy_simulation'],
                                            self.output_data['cost_avoidance_non_cumulative'])
