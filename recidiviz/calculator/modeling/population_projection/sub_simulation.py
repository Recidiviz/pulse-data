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
"""Simulate multiple demographic/age groups"""

from typing import Dict, Optional, List
import pandas as pd

from recidiviz.calculator.modeling.population_projection.compartment_transitions import CompartmentTransitions
from recidiviz.calculator.modeling.population_projection.shell_compartment import ShellCompartment
from recidiviz.calculator.modeling.population_projection.full_compartment import FullCompartment
from recidiviz.calculator.modeling.population_projection.incarceration_transitions import IncarceratedTransitions
from recidiviz.calculator.modeling.population_projection.release_transitions import ReleasedTransitions
from recidiviz.calculator.modeling.population_projection.spark_compartment import SparkCompartment
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
# TODO(#4512): test when no data is ingested into one of the compartments, outflows/durations initializations line up


class SubSimulation:
    """Run the population projection for one sub group"""

    def __init__(self, outflows_data: pd.DataFrame, transitions_data: pd.DataFrame, total_population_data: pd.DataFrame,
                 simulation_compartments: Dict[str, Optional[CompartmentTransitions]], user_inputs: Dict,
                 policy_list: List[SparkPolicy], first_relevant_ts, microsim: bool = False):

        # A DataFrame with historical outflows data
        self.outflows_data = outflows_data

        # A DataFrame with the historical stay lengths data
        self.transitions_data = transitions_data

        # A DataFrame with the historical total compartment populations data
        self.total_population_data = total_population_data

        # A Series with the total population error at start_ts
        self.start_ts_scale_factors = pd.Series(dtype=float)

        # A dict with the simulation compartment names and their corresponding transition table types
        self.simulation_architecture = simulation_compartments

        # Inputs from the user that control how the simulation is initialized
        self.user_inputs = user_inputs

        # A list of simulation policies
        self.policy_list = policy_list

        # The ts to begin the simulation following the historical data
        self.start_ts = user_inputs['start_time_step']

        # A dict of compartment tag (pre-trial, jail, prison, release...) to the corresponding SparkCompartment object
        self.simulation_compartments: Dict[str, SparkCompartment] = {}

        # units of subgroup max_sentence (ie 1 would be going 1 * max_sentence backward to initialize)
        self.first_relevant_ts = first_relevant_ts

        # indicates whether this is a micro-simulation
        self.microsim = microsim

    def initialize(self):
        """Initialize the transition tables, and then the compartments, for the SubSimulation"""
        # TODO(#4512): allow sparse data

        if not self.microsim and not self.total_population_data.empty:
            if self.user_inputs['start_time_step'] not in self.total_population_data.time_step.values:
                raise ValueError(f"Start time must be included in population data input\n"
                                 f"Expected: {self.user_inputs['start_time_step']}, "
                                 f"Actual: {self.total_population_data.time_step.values}")

        # Initialize a default transition class for each compartment to represent the no-policy scenario
        transitions_per_compartment = {}
        for compartment in self.simulation_architecture:
            transition_type = self.simulation_architecture[compartment]
            compartment_duration_data = self.transitions_data[self.transitions_data['compartment'] == compartment]

            if compartment_duration_data.empty:
                if transition_type is not None:
                    raise ValueError(f"Transition data missing for compartment {compartment}. Data is required for all "
                                     "disaggregtion axes. Even the 'release' compartment needs transition data even if "
                                     "it's just outflow to 'release'")
            else:
                if transition_type == 'incarcerated':
                    transition_class = IncarceratedTransitions(compartment_duration_data)
                elif transition_type == 'released':
                    transition_class = ReleasedTransitions(compartment_duration_data)
                else:
                    raise ValueError(f'unrecognized transition table type {transition_type}')

                transition_class.initialize_transition_table()
                transitions_per_compartment[compartment] = transition_class

        # Create a transition object for each compartment and year with policies applied and store shell policies
        shell_policies = dict()
        for compartment in self.simulation_architecture:
            # Select any policies that are applicable for this compartment
            compartment_policies = SparkPolicy.get_compartment_policies(self.policy_list, compartment)

            # add to the dict compartment -> transition class with policies applied
            if compartment in transitions_per_compartment:
                transitions_per_compartment[compartment].initialize(compartment_policies)

            # add shell policies to dict that gets passed to initialization
            else:
                shell_policies[compartment] = compartment_policies

        # Preprocess the historical data into separate pieces per compartment
        historical_outflows = self._load_data()

        # Initialize the compartment classes
        self._initialize_compartments(historical_outflows, transitions_per_compartment, shell_policies)

    def _load_data(self) -> pd.DataFrame:
        """pre-process historical outflows data to produce a dictionary of formatted DataFrames containing
        outflow data for each compartment"""

        # Check that the outflows data has the compartments needed
        simulation_architecture = self.simulation_architecture.keys()
        outflow_compartments = self.outflows_data['compartment'].unique()
        missing_compartment_data = [compartment for compartment in outflow_compartments
                                    if compartment not in simulation_architecture]
        if len(missing_compartment_data) != 0:
            raise ValueError(f"Simulation architecture is missing compartments for the outflows: "
                             f"{missing_compartment_data}")

        # get counts of population from historical data aggregated by compartment, outflow, and year
        preprocessed_data = self.outflows_data.groupby(['compartment', 'outflow_to', 'time_step'])[
            'total_population'].sum()

        preprocessed_data = preprocessed_data.unstack(level=['outflow_to', 'time_step'])
        preprocessed_data = preprocessed_data.reindex(simulation_architecture).stack(level='outflow_to', dropna=False)
        return preprocessed_data

    def _initialize_compartments(self, preprocessed_data: pd.DataFrame,
                                 transition_tables_by_compartment: Dict[str, CompartmentTransitions],
                                 shell_policies: Dict[str, List[SparkPolicy]]):
        """Initialize all the SparkCompartments for the subpopulation simulation"""

        if self.microsim:
            self.first_relevant_ts = int(self.start_ts)

        for compartment in self.simulation_architecture:
            transition_type = self.simulation_architecture[compartment]
            outflows_data = preprocessed_data.loc[compartment].dropna(axis=0).dropna(axis=1, how='all')

            # if no transition table, initialize as shell compartment
            if transition_type is None:
                if outflows_data.empty:
                    raise ValueError(f"outflows_data for shell compartment {compartment} cannot be empty")
                self.simulation_compartments[compartment] = ShellCompartment(
                    outflows_data=outflows_data,
                    starting_ts=self.first_relevant_ts,
                    policy_ts=self.user_inputs['policy_time_step'],
                    constant_admissions=self.user_inputs['constant_admissions'],
                    tag=compartment,
                    policy_list=shell_policies[compartment]
                )
            else:
                self.simulation_compartments[compartment] = FullCompartment(
                    outflow_data=outflows_data,
                    transition_tables=transition_tables_by_compartment[compartment],
                    starting_ts=self.first_relevant_ts,
                    policy_ts=self.user_inputs['policy_time_step'],
                    tag=compartment
                )

        for compartment in self.simulation_compartments:
            self.simulation_compartments[compartment].initialize_edges(list(self.simulation_compartments.values()))
            if self.microsim and isinstance(compartment, FullCompartment):
                compartment.microsim_initialize()

    def scale_total_populations(self):
        """scales populations of compartments in simulation. If a FullCompartment isn't included, it won't be scaled"""

        for compartment, populations in self.total_population_data.groupby('compartment'):
            self.start_ts_scale_factors[compartment] = \
                self.simulation_compartments[compartment].scale_cohorts(populations)

    def get_error(self, compartment='prison', unit='abs'):
        return self.simulation_compartments[compartment].get_error(unit=unit)

    def get_scale_factors(self):
        return self.start_ts_scale_factors

    def gen_arima_output_df(self):
        arima_output_df = pd.concat([compartment.gen_arima_output_df() for compartment
                                     in self.simulation_compartments.values()
                                     if isinstance(compartment, ShellCompartment)],
                                    sort=True)
        return arima_output_df

    def step_forward(self):
        """Run the simulation for one time step"""
        for compartment in self.simulation_compartments.values():
            compartment.step_forward()

        for compartment in self.simulation_compartments.values():
            compartment.prepare_for_next_step()

    def cross_flow(self):
        cohorts_table = pd.DataFrame(columns=['compartment'])
        for compartment_name, compartment_obj in self.simulation_compartments.items():
            if isinstance(compartment_obj, FullCompartment):
                compartment_cohorts = compartment_obj.get_cohort_df()
                compartment_cohorts['compartment'] = compartment_name
                cohorts_table = pd.concat([cohorts_table, compartment_cohorts], sort=True)

        return cohorts_table

    def ingest_cross_simulation_cohorts(self, cross_simulation_flows: pd.DataFrame):
        for compartment_obj in self.simulation_compartments.values():
            if isinstance(compartment_obj, FullCompartment):
                compartment_obj.ingest_cross_simulation_cohorts(cross_simulation_flows)

    def get_population_projections(self):
        """Return a DataFrame with the simulation population projections"""
        # combine the results into one DataFrame
        simulation_results = pd.DataFrame()
        for compartment_name, compartment in self.simulation_compartments.items():
            if isinstance(compartment, FullCompartment):
                compartment_results = pd.DataFrame(compartment.get_per_ts_population(), columns=['total_population'])
                compartment_results['compartment'] = compartment_name
                compartment_results['time_step'] = compartment_results.index
                simulation_results = pd.concat([simulation_results, compartment_results], sort=True)

        return simulation_results
