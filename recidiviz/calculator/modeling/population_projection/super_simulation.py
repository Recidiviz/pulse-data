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

from abc import ABC, abstractmethod
from typing import Dict, List, Any
from warnings import warn
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.population_simulation import PopulationSimulation
# pylint: disable=line-too-long


class SuperSimulation(ABC):
    """Manage the PopulationSimulations and output data needed to run tests, baselines, and policy scenarios"""

    def __init__(self, model_params_dict: Dict[str, Any]):
        self.pop_simulations: Dict[str, PopulationSimulation] = {}
        self.data_dict: Dict[str, Any] = {}
        self.output_data: Dict[str, pd.DataFrame] = {}

        self.reference_year = model_params_dict['reference_year']
        self.time_step = model_params_dict['time_step']

        self._initialize_data(model_params_dict['data_inputs_raw'])

        self.data_dict['simulation_compartments_architecture'] = \
            model_params_dict['simulation_compartments_architecture']

        self.data_dict['disaggregation_axes'] = model_params_dict['disaggregation_axes']

        self.compartment_costs = model_params_dict['compartment_costs']

        # Parse the simulation settings from the initialization parameters
        self._set_user_inputs(model_params_dict['user_inputs_raw'])

    def _convert_to_absolute_year(self, time_steps: pd.Series):
        """converts a number of time steps relative to reference date into absolute dates"""
        return time_steps.apply(lambda x: np.round(x * self.time_step + self.reference_year, 8))

    def _convert_to_relative_date(self, years: Any):
        """converts units of years to units of time steps"""
        ts = (years - self.reference_year) / self.time_step
        if not np.isclose(ts, round(ts)):
            raise ValueError(f"Cannot convert years {years} to integers {ts}")
        return round(ts)

    def _format_simulation_results(self, collapse_compartments=False) -> pd.DataFrame:
        """Re-format PopulationSimulation results so each simulation is a column"""
        simulation_results = pd.DataFrame()
        for scenario, simulation in self.pop_simulations.items():
            results = simulation.population_projections[simulation.population_projections.time_step >=
                                                        self.user_inputs['start_time_step']]
            results = results.rename({'time_step': 'year', 'total_population': f'{scenario}_total_population'}, axis=1)
            results.year = self._convert_to_absolute_year(results.year)

            if simulation_results.empty:
                simulation_results = results
            else:
                simulation_results = simulation_results.merge(
                    results, on=['compartment', 'year', 'simulation_group'])

        if collapse_compartments:
            simulation_results = simulation_results.groupby(['compartment', 'year'], as_index=False).sum()

        simulation_results.index = simulation_results.year

        return simulation_results

    @abstractmethod
    def _initialize_data(self, data_inputs_params: Dict[str, Any]):
        """Initialize the data_dict"""

    @abstractmethod
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

    def _reset_pop_simulations(self):
        self.pop_simulations = {}

    def simulate_baseline(self, display_compartments: List[str], validation_pairs: Dict[str, str] = None,
                          first_relevant_ts: int = None):
        """
        Calculates a baseline projection, returns transition error for a specific transition
        `display_compartments` are the compartment whose populations you wish to display
        `validation_pairs` should be a dict with key/value pairs corresponding to compartment/outflow_to transitions
            to calculate error for
        """
        self._reset_pop_simulations()

        if first_relevant_ts is not None and first_relevant_ts < self.user_inputs['start_time_step']:
            raise ValueError(f"first_relevant_ts ({first_relevant_ts}) must be less than start_time_step ({self.user_inputs['start_time_step']}")

            # Run one simulation for the min and max confidence interval
        for projection_type in ['min', 'max']:
            self.user_inputs['projection_type'] = projection_type
            self._simulate_baseline(simulation_title=f'baseline_{projection_type}',
                                    first_relevant_ts=first_relevant_ts)

        # Run one simulation for the middle interval
        self.user_inputs['projection_type'] = 'middle'
        self._simulate_baseline(simulation_title='baseline', first_relevant_ts=first_relevant_ts)

        simulation_results = self._format_simulation_results(collapse_compartments=True)

        display_results = pd.DataFrame(index=simulation_results.year.unique())
        for comp in display_compartments:
            if comp not in simulation_results.compartment.unique():
                warn(f"Display compartment not in simulation architecture: {comp}", Warning)
            else:
                display_results[comp] = \
                    simulation_results[(simulation_results.compartment == comp) & (
                            simulation_results.year >= self.user_inputs['start_time_step'])].baseline_total_population

        display_results.plot(title="Baseline Population Projection", ylabel="Estimated Total Population")
        plt.legend(loc='lower left')
        plt.ylim([0, None])

        if validation_pairs is not None:
            self.calculate_baseline_transition_error(validation_pairs)

    @abstractmethod
    def _simulate_baseline(self, simulation_title: str, first_relevant_ts: int = None):
        """
        Calculates a baseline projection, returns transition error for a specific transition
        `simulation_title` is the desired simulation tag for this baseline
        `first_relevant_ts` is the ts at which to start initialization
        """

        self.pop_simulations[simulation_title] = PopulationSimulation()
        self.user_inputs['policy_list'] = []

    def calculate_baseline_transition_error(self, validation_pairs: Dict[str, str]):
        self.output_data['baseline_transition_error'] = \
            pd.DataFrame(columns=['compartment', 'outflow', 'subgroup', 'year', 'error'])
        for compartment, outflow_to in validation_pairs.items():
            for sub_group in self.pop_simulations['baseline'].sub_simulations:
                error = pd.DataFrame(self.pop_simulations['baseline'].sub_simulations[
                                         sub_group].get_error(compartment)[outflow_to]).reset_index()
                error = error.rename({'time_step': 'year', outflow_to: 'error'}, axis=1)
                error['outflow'] = outflow_to
                error['compartment'] = compartment
                error['subgroup'] = sub_group

                self.output_data['baseline_transition_error'] = \
                    pd.concat([self.output_data['baseline_transition_error'], error])

        self.output_data['baseline_transition_error'].year = \
            self._convert_to_absolute_year(self.output_data['baseline_transition_error'].year)

        self.output_data['baseline_population_error'] = self.pop_simulations['baseline'].gen_scale_factors_df()

    def calculate_outflows_error(self, simulation_title: str):
        # TODO(#5444): re-factor using self.gen_arima_output_df
        outflows = pd.DataFrame()
        outflows['model'] = self.gen_arima_output_df(simulation_title).groupby(['compartment', 'outflow_to',
                                                                                'time_step']).pred_middle.sum()
        if 'run_date' in self.data_dict['outflows_data']:
            outflows_data = self.data_dict['outflows_data'][self.data_dict['outflows_data'].run_date ==
                                                            self.data_dict['outflows_data'].run_date.max()]
        else:
            outflows_data = self.data_dict['outflows_data']

        outflows['actual'] = outflows_data.groupby(['compartment', 'outflow_to', 'time_step']).total_population.sum()

        outflows.fillna(0)
        return outflows[outflows.index.get_level_values(level='time_step') >= self.user_inputs['start_time_step']]

    def gen_arima_output_df(self, simulation_title: str):
        return self.pop_simulations[simulation_title].gen_arima_output_df()

    def gen_arima_output_plots(self, simulation_title: str, fig_size=(8, 6), by_simulation_group: bool = False):
        """
        Generates admissions forecast plots broken up by compartment and outflow.
        `simulation_title` should be the tag of the PopulationSimulation of interest
        If by_simulation_group = False, plots are generated for each shell compartment and outflow_to compartment
        If by_simulation_group = True these are further subdivided by simulation group
        """
        arima_output_df = self.gen_arima_output_df(simulation_title)
        if not by_simulation_group:
            arima_output_df = arima_output_df.groupby(level=['compartment', 'outflow_to', 'time_step']).apply(
                lambda x: x.sum(skipna=False))
        levels_to_plot = [x for x in arima_output_df.index.names if x != 'time_step']
        dfs_to_plot = arima_output_df.groupby(levels_to_plot)

        axes = []
        for i, df_to_plot in dfs_to_plot:
            _, ax = plt.subplots(figsize=fig_size)
            sub_plot = df_to_plot.reset_index()
            sub_plot.index = self._convert_to_absolute_year(pd.Series(sub_plot.index))
            sub_plot['actuals'].plot(ax=ax, color='tab:cyan', marker='o', label='Actuals')
            sub_plot['pred_middle'].plot(ax=ax, color='tab:red', marker='o', label='Predictions')

            ax.fill_between(sub_plot.index, sub_plot['pred_min'], sub_plot['pred_max'], alpha=0.4, color='orange')

            plt.ylim(bottom=0, top=max([sub_plot.pred_middle.max(), sub_plot.actuals.max()]) * 1.1)
            plt.legend(loc='lower left')
            plt.title('\n'.join([': '.join(z) for z in zip(levels_to_plot, i)]))
            axes.append(ax)
        return axes

    def gen_total_population_error(self, simulation_type: str):
        # Convert the index from relative time steps to floating point years
        error_results = self.pop_simulations[simulation_type].gen_total_population_error()
        error_results.index = self._convert_to_absolute_year(pd.Series(error_results.index))
        return error_results

    def gen_full_error_output(self, simulation_type: str):
        error_results = self.pop_simulations[simulation_type].gen_full_error()
        # Convert the index from relative time steps to floating point years
        error_results.index = error_results.index.set_levels(
            self._convert_to_absolute_year(pd.Series(error_results.index.levels[1])), level=1)
        error_results['compartment_type'] = [x.split()[0] for x in error_results.index.get_level_values(0)]
        return error_results
