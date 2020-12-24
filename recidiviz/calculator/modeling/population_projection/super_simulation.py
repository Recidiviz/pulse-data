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
from io import TextIOWrapper
import yaml
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection import spark_bq_inputs
from recidiviz.calculator.modeling.population_projection.population_simulation import PopulationSimulation
from recidiviz.calculator.modeling.population_projection.spark_bq_upload import upload_spark_results
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class SuperSimulation:
    """Run the population projection simulation with or without a policy applied"""

    def __init__(self, yaml_file: TextIOWrapper):
        self.pop_simulations: Dict[str, PopulationSimulation] = {}
        self.data_dict = {}
        self.output_data: Dict[str, pd.DataFrame] = {}
        self.user_inputs: Dict[str, Any] = {}
        self.compartment_costs = {}
        self.microsim = None

        initialization_params = yaml.full_load(yaml_file)
        self.microsim = initialization_params['micro_simulation']

        # Make sure only one input setting is provided in the yaml file
        if sum([initialization_params.get('state_data') is not None,
                initialization_params.get('big_query_inputs') is not None]) != 1:
            raise ValueError("Only one option can be set in the yaml file: `state_data` OR `big_query_inputs`")

        self.reference_year = initialization_params['reference_date']
        self.time_step = initialization_params['time_step']

        if initialization_params.get('state_data', False):
            simulation_data = pd.read_csv(initialization_params['state_data'])
            self._initialize_data_from_csv(simulation_data)
        else:
            self._initialize_data_from_big_query(initialization_params['big_query_inputs'])

        model_architecture_yaml_key = 'model_architecture'
        self.data_dict['simulation_compartments_architecture'] = initialization_params[model_architecture_yaml_key]
        self.data_dict['disaggregation_axes'] = initialization_params['disaggregation_axes']

        # Parse the simulation settings from the initialization parameters
        self._set_user_inputs(initialization_params['user_inputs'])

        compartment_costs_key = 'per_ts_costs'
        self.compartment_costs = initialization_params[compartment_costs_key]

        # Ensure there are compartment costs for every compartment in the model architecture
        model_compartments = set(c for c in self.data_dict['simulation_compartments_architecture']
                                 if self.data_dict['simulation_compartments_architecture'][c] is not None)
        compartment_costs = set(self.compartment_costs.keys())
        if compartment_costs != model_compartments:
            raise ValueError(
                f"Compartments do not match in the YAML '{compartment_costs_key}' and '{model_architecture_yaml_key}'\n"
                f"Mismatched values: {compartment_costs ^ model_compartments}"
            )

    def _convert_to_absolute_year(self, time_steps: Any):
        """converts a number of time steps relative to reference date into absolute dates"""
        return time_steps * self.time_step + self.reference_year

    def _convert_to_relative_date(self, years: Any):
        """converts units of years to units of time steps"""
        ts = (years - self.reference_year) / self.time_step
        if not np.isclose(ts, round(ts)):
            raise ValueError(f"Cannot convert years {years} to integers {ts}")
        return round(ts)

    def _convert_to_relative_date_from_timestamp(self, timestamp: pd.datetime):
        """converts units of years to units of time steps"""
        reference_date_year = np.floor(self.reference_year)
        reference_date_month = 12 * (self.reference_year % 1) + 1
        ts = 12 * (timestamp.year - reference_date_year) + timestamp.month - reference_date_month
        if not np.isclose(ts, round(ts)):
            raise ValueError(f"Cannot convert date {timestamp} to integer {ts}")
        return round(ts)

    def _format_simulation_results(self, results: pd.DataFrame, simulation_type: str):
        grouped_results = \
            results.groupby(['compartment', 'time_step'], as_index=False).agg({'total_population': ['sum']})
        grouped_results = grouped_results.rename({
            'total_population': f'{simulation_type}_total_population', 'time_step': 'year'}, axis=1)
        grouped_results.columns = grouped_results.columns.droplevel(1)
        grouped_results['year'] = self._convert_to_absolute_year(grouped_results['year'])
        return grouped_results

    def _initialize_data_from_csv(self, simulation_data: pd.DataFrame):
        """Initialize the data_dict from the CSV data"""
        if not self.microsim:
            transitions_data = simulation_data[~simulation_data.compartment_duration.isnull()]
            outflows_data = simulation_data[(simulation_data.compartment_duration.isnull()) &
                                            (~simulation_data.outflow_to.isnull())]
            total_population_data = simulation_data[simulation_data.outflow_to.isnull()]

        else:
            remaining_sentence_data = simulation_data[~simulation_data.remaining_duration.isnull()]
            transitions_data = simulation_data[(~simulation_data.compartment_duration.isnull()) &
                                               (simulation_data.remaining_duration.isnull())]
            outflows_data = simulation_data[(simulation_data.compartment_duration.isnull()) &
                                            (~simulation_data.outflow_to.isnull())]
            total_population_data = simulation_data[simulation_data.outflow_to.isnull()]

            self.data_dict['remaining_sentence_data'] = remaining_sentence_data

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

    def _initialize_data_from_big_query(self, big_query_params: Dict[str, str]):
        project_id = big_query_params['project_id']
        dataset = big_query_params['input_dataset']
        state_code = big_query_params['state_code']
        run_date = big_query_params['run_date']

        input_data_tables = ['outflows_data', 'transitions_data', 'total_population_data', 'remaining_sentence_data']
        for table_key in input_data_tables:
            table_name = big_query_params[table_key]
            table_data = spark_bq_inputs.load_table_from_big_query(project_id, dataset, table_name, state_code,
                                                                   run_date)
            if 'time_step' in table_data.columns:
                # Convert the time_step from a timestamp to a relative int value
                table_data['time_step'] = table_data['time_step'].apply(self._convert_to_relative_date_from_timestamp)

            print(f"{table_key} for {table_name} returned {len(table_data)} results")
            self.data_dict[table_key] = table_data

        # add extra transitions from the RELEASE compartment
        self.data_dict['transitions_data'] = spark_bq_inputs.add_transition_rows(self.data_dict['transitions_data'])

        self.data_dict['remaining_sentence_data'] = spark_bq_inputs.add_remaining_sentence_rows(
            self.data_dict['remaining_sentence_data']
        )

    def _set_user_inputs(self, yaml_user_inputs: Dict[str, Any]):
        self.user_inputs = dict()
        self.user_inputs['start_time_step'] = \
            self._convert_to_relative_date(yaml_user_inputs['start_year'])
        self.user_inputs['projection_time_steps'] = yaml_user_inputs['projection_years'] / self.time_step
        if not np.isclose(self.user_inputs['projection_time_steps'], round(self.user_inputs['projection_time_steps'])):
            raise ValueError(f"Projection years {yaml_user_inputs['projection_years']} input cannot be evenly divided "
                             f"by time step {self.time_step}")
        self.user_inputs['projection_time_steps'] = round(self.user_inputs['projection_time_steps'])

        # Load all optional arguments, set them to the default value if not provided in the initialization params
        self.user_inputs['constant_admissions'] = yaml_user_inputs.get('constant_admissions', False)
        self.user_inputs['speed_run'] = yaml_user_inputs.get('speed_run', False)

        if not self.microsim:
            self.user_inputs['policy_time_step'] = \
                self._convert_to_relative_date(yaml_user_inputs['policy_year'])
            if 'policy_list' not in yaml_user_inputs:
                self.user_inputs['policy_list'] = []
            else:
                self.user_inputs['policy_list'] = yaml_user_inputs['policy_list']

        else:
            #this will be populated in the PopulationSimulation
            self.user_inputs['policy_time_step'] = self.user_inputs['start_time_step'] + 1
            self.user_inputs['policy_list'] = []

    def _reset_pop_simulations(self):
        self.pop_simulations = {}

    def simulate_policy(self, policy_list: List[SparkPolicy], output_compartment: str):
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

        self.pop_simulations['policy'].simulate_policies(*simulation_data_inputs, self.user_inputs)

        control_user_inputs = deepcopy(self.user_inputs)
        control_user_inputs['policy_list'] = []
        self.pop_simulations['control'].simulate_policies(*simulation_data_inputs, control_user_inputs)

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

    def simulate_baseline(self, output_compartment: str, outflow_to: str, initialization_period: float = None):
        """
        Calculates a baseline projection, returns transition error for a specific transition
        `output_compartment` is the compartment whose error you want to get
        `outflow_to` is the outflow from that compartment you want to get the error on
        """

        if self.microsim:
            if initialization_period is not None:
                raise ValueError("Cannot specify initialization_period for microsimulations")

        self._reset_pop_simulations()

        self.pop_simulations['baseline'] = PopulationSimulation()

        self.user_inputs['policy_list'] = []

        if self.microsim:
            simulation_data_inputs = (
                self.data_dict['outflows_data'],
                self.data_dict['remaining_sentence_data'],
                self.data_dict['total_population_data'],
                self.data_dict['simulation_compartments_architecture'],
                self.data_dict['disaggregation_axes']
            )
        else:
            simulation_data_inputs = (
                self.data_dict['outflows_data'],
                self.data_dict['transitions_data'],
                self.data_dict['total_population_data'],
                self.data_dict['simulation_compartments_architecture'],
                self.data_dict['disaggregation_axes']
            )

        if initialization_period is None and self.microsim:
            self.pop_simulations['baseline'].simulate_policies(*simulation_data_inputs, self.user_inputs, microsim=True,
                                                               microsim_data=self.data_dict['transitions_data'])
        elif initialization_period is None and not self.microsim:
            self.pop_simulations['baseline'].simulate_policies(*simulation_data_inputs, self.user_inputs,
                                                               microsim=False)
        else:
            self.pop_simulations['baseline'].simulate_policies(*simulation_data_inputs, self.user_inputs,
                                                               initialization_period)

        self.output_data['baseline_transition_error'] = pd.DataFrame()
        for sub_group in self.pop_simulations['baseline'].sub_simulations:
            self.output_data['baseline_transition_error'][sub_group] = \
                self.pop_simulations['baseline'].sub_simulations[sub_group].get_error(output_compartment)[outflow_to]

        self.output_data['baseline_transition_error'].index = \
            self._convert_to_absolute_year(self.output_data['baseline_transition_error'].index)

        self.output_data['baseline_population_error'] = self.pop_simulations['baseline'].gen_scale_factors_df()

        self.output_data['baseline'] = self.pop_simulations['baseline'].population_projections.sort_values('time_step')

        simulation_results = self._format_simulation_results(self.output_data['baseline'], 'baseline')

        simulation_results[(simulation_results.compartment == outflow_to) &
                           (simulation_results.year >= self.user_inputs['start_time_step'])].plot(
            x='year', y='baseline_total_population')
        plt.title(f"Population projection for {outflow_to} Population")
        plt.ylabel(f"Estimated Year End {outflow_to} Population")
        plt.legend(loc='lower left')
        plt.ylim([0, None])

        return self.output_data['baseline_transition_error'], self.output_data['baseline_population_error']

    def calculate_cohort_population_error(self, output_compartment: str, outflow_to: str,
                                          back_fill_range: tuple = (0, 2, 0.1), unit: str = 'abs'):
        """
        `backfill_range` is a three item tuple giving the lower and upper bounds to test in units of
            subgroup max_sentence and the step size
        `output_compartment` is the compartment whose error you want to get
        `outflow_to` is the outflow from that compartment you want to get the error on
        `unit is either mse or abs`
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
                initialization_period=ts
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

    def gen_arima_output_df(self):
        return next(iter(self.pop_simulations.values())).gen_arima_output_df()

    def gen_arima_output_plots(self, fig_size=(8, 6), by_simulation_group:bool = False):
        """
        Generates admissions forecast plots broken up by compartment and outflow.
        If by_simulation_group = False, plots are generated for each shell compartment and outflow_to compartment
        If by_simulation_group = True these are further subdivided by simulation group
        """
        arima_output_df = self.gen_arima_output_df()
        if not by_simulation_group:
            arima_output_df = arima_output_df.groupby(level=['compartment', 'outflow_to', 'time_step']).apply(
                lambda x: x.sum(skipna=False))
        levels_to_plot = [x for x in arima_output_df.index.names if x != 'time_step']
        dfs_to_plot = arima_output_df.groupby(levels_to_plot)

        axes = []
        for i, df_to_plot in dfs_to_plot:
            _, ax = plt.subplots(figsize=fig_size)
            sub_plot = df_to_plot.reset_index()
            sub_plot.index = sub_plot.index.map(self._convert_to_absolute_year)
            sub_plot['actuals'].plot(ax=ax, color='tab:cyan', marker='o', label='Actuals')
            sub_plot['pred'].plot(ax=ax, color='tab:red', marker='o', label='Predictions')

            ax.fill_between(sub_plot.index, sub_plot['pred_min'], sub_plot['pred_max'], alpha=0.4, color='orange')

            plt.ylim(bottom=0, top=max([sub_plot.pred.max(), sub_plot.actuals.max()]) * 1.1)
            plt.legend(loc='lower left')
            plt.title('\n'.join([': '.join(z) for z in zip(levels_to_plot, i)]))
            axes.append(ax)
        return axes

    def gen_total_population_error(self, simulation_type: str):
        # Convert the index from relative time steps to floating point years
        error_results = self.pop_simulations[simulation_type].gen_total_population_error()
        error_results.index = error_results.index.map(self._convert_to_absolute_year)
        return error_results

    def upload_simulation_results_to_bq(self, project_id, simulation_tag):
        required_keys = ['policy_simulation', 'cost_avoidance', 'life_years', 'cost_avoidance_non_cumulative']
        missing_keys = [key for key in required_keys if key not in self.output_data.keys()]
        if len(missing_keys) != 0:
            raise ValueError(f"Output data is missing the required columns {missing_keys}")
        upload_spark_results(project_id, simulation_tag, self.output_data['cost_avoidance'],
                             self.output_data['life_years'], self.output_data['policy_simulation'],
                             self.output_data['cost_avoidance_non_cumulative'])
