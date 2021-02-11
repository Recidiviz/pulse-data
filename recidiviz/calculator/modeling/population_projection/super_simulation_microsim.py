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
from typing import Dict, List, Any
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation import SuperSimulation
from recidiviz.calculator.modeling.population_projection import ignite_bq_utils
from recidiviz.calculator.modeling.population_projection.simulations.super_simulation.super_simulation_initializer \
    import SuperSimulationInitializer
from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.population_simulation \
    import PopulationSimulation


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
                             'total_jail_population_data', 'total_out_of_state_supervised_population_data']
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

    def _build_population_simulation(self) -> PopulationSimulation:
        return SuperSimulationInitializer.build_population_simulation({
            'population_simulation': {'initializer': 'micro'},
            'sub_simulation': {'initializer': 'micro'}
        })

    def _simulate_baseline(self, simulation_title: str, first_relevant_ts: int = None):
        """
        Calculates a baseline projection, returns transition error for a specific transition
        `simulation_title` is the desired tag of the PopulationSimulation
        `first_relevant_ts` should always be null
        """
        super()._simulate_baseline(simulation_title, first_relevant_ts)

        first_relevant_ts = int(self.user_inputs['start_time_step'])

        simulation_data_inputs = (
            self.data_dict['outflows_data'][
                self.data_dict['outflows_data'].run_date == self.user_inputs['run_date']],
            self.data_dict['remaining_sentence_data'][
                self.data_dict['remaining_sentence_data'].run_date == self.user_inputs['run_date']],
            self.data_dict['total_population_data'][
                self.data_dict['total_population_data'].run_date == self.data_dict[
                    'total_population_data'].run_date.max()],
            self.data_dict['compartments_architecture'],
            self.data_dict['disaggregation_axes']
        )

        self.pop_simulations[simulation_title].simulate_policies(*simulation_data_inputs, self.user_inputs,
                                                                 first_relevant_ts=first_relevant_ts,
                                                                 microsim_data=self.data_dict['transitions_data'])

        self.output_data[simulation_title] = \
            self.pop_simulations[simulation_title].population_projections.sort_values('time_step')

    def microsim_baseline_over_time(self, start_run_dates: List[datetime]):
        """
        Run a microsim at many different run_dates.
        `start_run_dates` should be a list of datetime at which to run the simulation
        """
        self._reset_pop_simulations()

        for start_date in start_run_dates:
            self.user_inputs['run_date'] = start_date
            self.pop_simulations[f"start date: {start_date}"] = self._build_population_simulation()
            self._simulate_baseline(simulation_title=f"start date: {start_date}")

    def _prep_for_upload(self, projection_data: pd.DataFrame):
        """function for scaling and any other state-specific operations required pre-upload"""
        scalar_dict = {
            'SUPERVISION - PROBATION': self._calculate_prep_scale_factor(
                'total_out_of_state_supervised_population_data', 'SUPERVISION - PROBATION'),
            'SUPERVISION - PAROLE': self._calculate_prep_scale_factor(
                'total_out_of_state_supervised_population_data', 'SUPERVISION - PAROLE'),
            'INCARCERATION - GENERAL': self._calculate_prep_scale_factor(
                'total_jail_population_data', 'INCARCERATION - GENERAL'),
            'INCARCERATION - RE-INCARCERATION': self._calculate_prep_scale_factor(
                'total_jail_population_data', 'INCARCERATION - RE-INCARCERATION'),
            'INCARCERATION - PAROLE_BOARD_HOLD': self._calculate_prep_scale_factor(
                'total_jail_population_data', 'INCARCERATION - PAROLE_BOARD_HOLD'),
            'INCARCERATION - TREATMENT_IN_PRISON': self._calculate_prep_scale_factor(
                'total_jail_population_data', 'INCARCERATION - TREATMENT_IN_PRISON'),
        }
        print(scalar_dict)

        output_data = projection_data.copy()
        output_data['scale_factor'] = output_data.compartment.map(scalar_dict).fillna(1)
        output_data.loc[:, ['total_population', 'total_population_min', 'total_population_max']] *= \
            output_data.scale_factor
        output_data = output_data.drop('scale_factor', axis=1)

        return output_data

    def _calculate_prep_scale_factor(self, data_tag: str, compartment_tag: str):
        """helper function for _prep_for_upload"""
        excluded_pop = self.data_dict[data_tag]
        excluded_pop = excluded_pop[(excluded_pop.time_step == self.user_inputs['start_time_step']) &
                                    (excluded_pop.run_date == self.user_inputs['run_date']) &
                                    (excluded_pop.compartment == compartment_tag)]
        excluded_pop = excluded_pop.total_population.iloc[0]

        total_pop = self.data_dict['total_population_data']
        total_pop = total_pop[(total_pop.time_step == self.user_inputs['start_time_step']) &
                              (total_pop.run_date == self.user_inputs['run_date']) &
                              (total_pop.compartment == compartment_tag)]
        total_pop = total_pop.total_population.sum()

        return 1 - excluded_pop / total_pop

    def upload_microsim_results_to_bq(self, project_id, simulation_tag):
        required_keys = ['baseline', 'baseline_min', 'baseline_max']
        missing_keys = [key for key in required_keys if key not in self.output_data.keys()]
        if len(missing_keys) != 0:
            raise ValueError(f"Microsim output data is missing the required columns {missing_keys}")

        join_cols = ['time_step', 'compartment', 'simulation_group']
        microsim_data = self.output_data['baseline'].merge(
            self.output_data['baseline_min'], on=join_cols, suffixes=['', '_min']
        ).merge(self.output_data['baseline_max'], on=join_cols, suffixes=['', '_max'])

        microsim_data['year'] = microsim_data['time_step'].apply(self._convert_to_absolute_year)
        microsim_data = microsim_data.drop('time_step', axis=1)
        microsim_data = self._prep_for_upload(microsim_data)
        ignite_bq_utils.upload_ignite_results(project_id, simulation_tag, microsim_data)
