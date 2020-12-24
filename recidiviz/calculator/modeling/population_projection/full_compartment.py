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
"""SparkCompartment that tracks cohorts internally to determine population size and outflows"""

from typing import Dict
import pandas as pd
import numpy as np

from recidiviz.calculator.modeling.population_projection.cohort_table import CohortTable
from recidiviz.calculator.modeling.population_projection.spark_compartment import SparkCompartment
from recidiviz.calculator.modeling.population_projection.compartment_transitions import CompartmentTransitions


class FullCompartment(SparkCompartment):
    """Complex Spark Compartment that tracks cohorts over time and sends groups to other compartments over time"""

    def __init__(self, outflow_data: pd.DataFrame, transition_tables: CompartmentTransitions, starting_ts: int,
                 policy_ts: int, tag: str):

        super().__init__(outflow_data, starting_ts, policy_ts, tag)

        # store all population cohorts with their population counts per ts
        self.cohorts: CohortTable = CohortTable(starting_ts, transition_tables.max_sentence)

        # separate incoming cohorts that should be processed after .step_forward()
        self.incoming_cohorts: float = 0

        # transition tables object from compartment out
        self.transition_tables = transition_tables

        # Series containing compartment population at the end of each ts in the simulation
        self.end_ts_populations = pd.Series(dtype=float)

    def microsim_initialize(self):
        """populate cohort table with single starting cohort of microsim"""
        starting_size = self.transition_tables.historical_outflows['total_population'].sum()
        self.cohorts.append_ts_end_count(self.cohorts.get_latest_population(), self.current_ts)
        self.ingest_incoming_cohort({self.tag: starting_size})
        self.prepare_for_next_step()

    def _generate_outflow_dict(self):
        """step forward all cohorts one time step and generate outflow dict"""

        per_ts_transitions, long_sentence_transitions = self.transition_tables.get_per_ts_transition_table(
            self.current_ts, self.policy_ts)

        latest_ts_pop = self.cohorts.get_latest_population()

        # convert index from starting year to years in compartment
        latest_ts_pop.index = self.current_ts - latest_ts_pop.index

        # no cohort should start in cohort after current_ts
        if any(latest_ts_pop.index <= 0):
            raise ValueError("Cohort cannot start after current time step\n"
                             f"Current time step: {self.current_ts}\n"
                             f"Cohort start times: {self.current_ts - latest_ts_pop.index}")

        latest_ts_pop_short = latest_ts_pop[latest_ts_pop.index <= len(per_ts_transitions)]
        latest_ts_pop_long = pd.DataFrame(latest_ts_pop[latest_ts_pop.index > len(per_ts_transitions)])
        if not latest_ts_pop_long.empty:
            latest_ts_pop_long.columns = [0]

        # broadcast latest cohort populations onto transition table
        latest_ts_pop_short = per_ts_transitions.mul(latest_ts_pop_short, axis=0)

        # dot latest long sentence cohort populations with long sentence probabilities
        latest_ts_pop_long = latest_ts_pop_long.dot(long_sentence_transitions)

        latest_ts_pop = latest_ts_pop_long.append(latest_ts_pop_short).fillna(0)

        # convert index back to starting year from years in compartment
        latest_ts_pop.index = self.current_ts - latest_ts_pop.index

        self.cohorts.append_ts_end_count(latest_ts_pop['remaining'].sort_index(), self.current_ts)

        outflow_dict = {outflow: latest_ts_pop.sum(axis=0)[outflow] for outflow in latest_ts_pop.columns
                        if outflow not in ['death', 'remaining']}
        return outflow_dict

    def ingest_incoming_cohort(self, influx: Dict[str, int]):
        """Ingest the population coming from one compartment into another by the end of the `current_ts`

        influx: dictionary of cohort type (str) to number of people revoked for the ts (int)
        """
        if self.tag in influx.keys() and influx[self.tag] != 0:
            self.incoming_cohorts += influx[self.tag]

    def ingest_cross_simulation_cohorts(self, cross_simulation_flows: pd.DataFrame):
        self.cohorts.ingest_cross_simulation_cohorts(
            cross_simulation_flows[cross_simulation_flows.compartment == self.tag].drop('compartment', axis=1))

    def step_forward(self):
        """Simulate one time step in the projection"""
        super().step_forward()
        outflow_dict = self._generate_outflow_dict()
        self.outflows[self.current_ts] = pd.Series(outflow_dict, dtype=float)

        # if historical data available, use that instead
        if self.current_ts in self.outflows_data.columns and self.current_ts < self.policy_ts:
            # TODO(#4512): correct released population so calculated influx matches observed influx
            model_outflow = pd.Series(outflow_dict, dtype=float)
            outflow_dict = self.outflows_data[self.current_ts].to_dict()
            self.error[self.current_ts] = 100 * (model_outflow - self.outflows_data[self.current_ts]) / \
                self.outflows_data[self.current_ts]

        # if prior to historical data, interpolate from earliest ts of data
        elif not self.outflows_data.empty and self.current_ts < min(self.outflows_data.columns) \
                and self.current_ts < self.policy_ts:
            # TODO(#4512): replace outflows during initialization with better backward projection
            outflow_dict = self.outflows_data[min(self.outflows_data.columns)].to_dict()

        # if no outflow, don't ingest to edges
        if not bool(outflow_dict):
            return

        for edge in self.edges:
            edge.ingest_incoming_cohort(outflow_dict)

    def prepare_for_next_step(self):
        """Clean up any data structures and move the time step 1 unit forward"""
        # move the incoming cohort into the cohorts list
        self.cohorts.append_cohort(self.incoming_cohorts, self.current_ts)
        self.incoming_cohorts = 0

        if self.current_ts in self.end_ts_populations:
            raise ValueError(f"Cannot prepare_for_next_step() if population already recorded for this time step \n"
                             f"time step {self.current_ts} already in end_ts_populations {self.end_ts_populations}")
        self.end_ts_populations = \
            self.end_ts_populations.append(pd.Series({self.current_ts: self.get_current_population()}))

        super().prepare_for_next_step()

    def scale_cohorts(self, total_populations: pd.DataFrame):

        total_population = total_populations.set_index('time_step').loc[self.current_ts, 'total_population']

        scale_factor = total_population / self.get_current_population()

        self.cohorts.scale_cohort_size(scale_factor)

        latest_modeled_population = self.get_current_population()
        if not np.isclose(total_population, latest_modeled_population):
            raise ValueError(f"Scaling failed, total_population must equal corresponding model population\n"
                             f"Expected: {total_population}, Actual: {latest_modeled_population}")
        return scale_factor

    def get_per_ts_population(self):
        """Return the per_ts projected population as a pd.Series of counts per EOTS"""
        return self.cohorts.get_per_ts_population()

    def get_current_population(self):
        return self.cohorts.get_latest_population().sum()

    def get_cohort_df(self):
        return self.cohorts.pop_cohorts()
