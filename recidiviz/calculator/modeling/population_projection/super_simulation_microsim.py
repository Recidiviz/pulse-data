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

from datetime import datetime
from warnings import warn
from typing import Dict, Any, Optional
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation import SuperSimulation
from recidiviz.calculator.modeling.population_projection import ignite_bq_utils
from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.\
    population_simulation_factory import PopulationSimulationFactory
from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.population_simulation \
    import PopulationSimulation
# pylint: disable=unused-variable


class MicroSuperSimulation(SuperSimulation):
    """Run the population projection simulation with or without a policy applied"""

    def _convert_to_relative_date_from_timestamp(self, timestamp: datetime):
        """converts units of years to units of time steps"""
        reference_date_year = np.floor(self.reference_year)
        reference_date_month = 12 * (self.reference_year % 1) + 1
        ts = 12 * (timestamp.year - reference_date_year) + timestamp.month - reference_date_month
        if not np.isclose(ts, round(ts)):
            raise ValueError(f"Cannot convert date {timestamp} to integer {ts}")
        return round(ts)

    def _initialize_data(self, data_inputs_params: Dict[str, Any]):
        """Initialize the data_dict from Big Query"""
        big_query_params = data_inputs_params['big_query_inputs']
        project_id = big_query_params['project_id']
        dataset = big_query_params['input_dataset']
        state_code = big_query_params['state_code']

        input_data_tables = ['outflows_data', 'transitions_data', 'total_population_data', 'remaining_sentence_data',
                             'excluded_population_data']
        for table_key in input_data_tables:
            table_name = big_query_params[table_key]
            table_data = ignite_bq_utils.load_ignite_table_from_big_query(project_id, dataset, table_name, state_code)
            if 'time_step' in table_data.columns:
                # Convert the time_step from a timestamp to a relative int value
                table_data['time_step'] = table_data['time_step'].apply(self._convert_to_relative_date_from_timestamp)

            print(f"{table_key} for {table_name} returned {len(table_data)} results")
            self.data_dict[table_key] = table_data

        # add extra transitions from the RELEASE compartment
        self.data_dict['transitions_data'] = ignite_bq_utils.add_transition_rows(self.data_dict['transitions_data'])

        self.data_dict['remaining_sentence_data'] = ignite_bq_utils.add_remaining_sentence_rows(
            self.data_dict['remaining_sentence_data']
        )

    def _set_user_inputs(self, yaml_user_inputs: Dict[str, Any]):
        super()._set_user_inputs(yaml_user_inputs)
        self.user_inputs['run_date'] = yaml_user_inputs['run_date']
        self.user_inputs['policy_time_step'] = self.user_inputs['start_time_step'] + 1

        # this will be populated in the PopulationSimulation
        self.user_inputs['policy_list'] = []

    def _build_population_simulation(self,
                                     first_relevant_ts_override: Optional[int] = None,
                                     outflows_data_override: Optional[pd.DataFrame] = None,
                                     user_inputs: Dict[str, Any] = None
                                     ) -> PopulationSimulation:
        if user_inputs is not None:
            raise ValueError("Cannot pass user inputs to micro-simulation")

        if first_relevant_ts_override is not None:
            raise ValueError("Cannot pass first_relevant_ts_override to micro-simulation")

        if outflows_data_override is not None:
            raise RuntimeError("Cannot pass outflows_data_override to microsimulation")

        run_date = self.user_inputs['run_date']

        # Handle sparse outflow events where a disaggregation is missing data for some time steps
        fully_connected_columns = self.data_dict['disaggregation_axes'] + ['compartment', 'outflow_to', 'time_step']
        outflows_data = self.data_dict['outflows_data'][self.data_dict['outflows_data'].run_date == run_date]
        outflows_data = outflows_data.groupby(fully_connected_columns)['total_population'].sum().unstack(
            level=['time_step'])

        # Raise a warning if there are any disaggregations without outflow records for more than 25% of the time steps
        missing_event_threshold = 0.25
        number_of_missing_events = outflows_data.isnull().sum(axis=1)
        sparse_disaggregations = number_of_missing_events[
            number_of_missing_events / len(outflows_data.columns) > missing_event_threshold]
        if not sparse_disaggregations.empty:
            warn(f"Outflows data is missing for more than {missing_event_threshold * 100}% for some disaggregations:\n"
                 f"{100 * sparse_disaggregations / len(outflows_data.columns)}")

        # Fill the total population with 0 and remove the multiindex for the population simulation
        outflows_data = outflows_data.fillna(0).stack('time_step').reset_index(name='total_population')

        return PopulationSimulationFactory.build_population_simulation(
            outflows_data=outflows_data,
            transitions_data=self.data_dict['remaining_sentence_data'][
                self.data_dict['remaining_sentence_data'].run_date == run_date],
            total_population_data=self.data_dict['total_population_data'][
                self.data_dict['total_population_data'].run_date == self.data_dict[
                    'total_population_data'].run_date.max()],
            simulation_compartments=self.data_dict['compartments_architecture'],
            disaggregation_axes=self.data_dict['disaggregation_axes'],
            user_inputs=self.user_inputs,
            first_relevant_ts=self.user_inputs['start_time_step'],
            microsim_data=self.data_dict['transitions_data'],
            should_initialize_compartment_populations=True,
            should_scale_populations_after_step=False
        )

    def _prep_for_upload(self, projection_data: pd.DataFrame):
        """function for scaling and any other state-specific operations required pre-upload"""

        scalar_dict = self._calculate_prep_scale_factor()
        print(scalar_dict)

        output_data = projection_data.copy()
        output_data['scale_factor'] = output_data.compartment.map(scalar_dict).fillna(1)

        error_scale_factors = output_data[output_data['scale_factor'] == 0]
        if not error_scale_factors.empty:
            raise ValueError(f"The scale factor cannot be 0 for the population scaling: {error_scale_factors}")

        scaling_columns = ['total_population', 'total_population_min', 'total_population_max']
        output_data.loc[:, scaling_columns] = output_data.loc[:, scaling_columns].multiply(
            output_data.loc[:, 'scale_factor'], axis="index")
        output_data = output_data.drop('scale_factor', axis=1)

        return output_data

    def _calculate_prep_scale_factor(self):
        """Compute the scale factor per compartment by calculating the fraction of the initial total population
        that should have been excluded.
        """
        excluded_pop = self.data_dict['excluded_population_data']
        excluded_pop = excluded_pop[(excluded_pop.time_step == self.user_inputs['start_time_step']) &
                                    (excluded_pop.run_date == self.user_inputs['run_date'])]

        total_pop = self.data_dict['total_population_data']
        total_pop = total_pop[(total_pop.time_step == self.user_inputs['start_time_step']) &
                              (total_pop.run_date == self.user_inputs['run_date'])]

        # Make sure there is only one row per compartment
        if excluded_pop['compartment'].nunique() != len(excluded_pop['compartment']):
            raise ValueError(f"Excluded population has duplicate rows for compartments: {excluded_pop}")

        scale_factors = dict()
        for _index, row in excluded_pop:
            compartment_total_pop = total_pop[total_pop.compartment == row.compartment].total_population.sum()
            scale_factors[row.compartment] = 1 - row.total_population / compartment_total_pop

        return scale_factors

    def upload_microsim_results_to_bq(self, project_id, simulation_tag):
        required_keys = ['baseline_middle', 'baseline_min', 'baseline_max']
        missing_keys = [key for key in required_keys if key not in self.output_data.keys()]
        if len(missing_keys) != 0:
            raise ValueError(f"Microsim output data is missing the required columns {missing_keys}")

        join_cols = ['time_step', 'compartment', 'simulation_group']
        microsim_data = self.output_data['baseline_middle'].merge(
            self.output_data['baseline_min'], on=join_cols, suffixes=['', '_min']
        ).merge(self.output_data['baseline_max'], on=join_cols, suffixes=['', '_max'])

        microsim_data['year'] = self._convert_to_absolute_year(microsim_data['time_step'])
        microsim_data = microsim_data.drop('time_step', axis=1)
        microsim_data = self._prep_for_upload(microsim_data)
        ignite_bq_utils.upload_ignite_results(project_id, simulation_tag, microsim_data)
