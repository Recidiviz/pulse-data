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
"""SparkCompartment instance that doesn't track cohorts"""

from typing import Dict, List
import pandas as pd

from recidiviz.calculator.modeling.population_projection.predicted_admissions import PredictedAdmissions
from recidiviz.calculator.modeling.population_projection.spark_compartment import SparkCompartment


class ShellCompartment(SparkCompartment):
    """Simple Spark Compartment that only sends groups to other compartments and does not ingest cohorts"""

    def __init__(self, outflows_data: pd.DataFrame, starting_ts: int, policy_ts: int, tag: str, policy_list: list,
                 constant_admissions: bool):

        super().__init__(outflows_data, starting_ts, policy_ts, tag)

        self.policy_list = policy_list

        self.after_data = outflows_data.copy()
        for policy in policy_list:
            policy.policy_fn(self)

        self.admissions_predictors = {
            'before': PredictedAdmissions(outflows_data, constant_admissions),
            'after': PredictedAdmissions(self.after_data, constant_admissions)
        }

    def initialize_edges(self, edges: List[SparkCompartment]):
        """checks all compartments this outflows to are in `self.edges`"""
        edge_tags = [i.tag for i in edges]
        if not set(self.outflows_data.index).issubset(edge_tags):
            raise ValueError(f"Some edges are not supported in the outflows data"
                             f"Expected:{self.outflows_data.index}, Actual: {edge_tags}")
        super().initialize_edges(edges)

    def ingest_incoming_cohort(self, influx: Dict[str, int]):
        """Ingest the population coming from one compartment into another by the end of the `current ts`

        influx: dictionary of cohort type (str) to number of people revoked for the time period (int)
        """
        if self.tag in influx:
            raise ValueError(f"Shell compartment {self.tag} cannot ingest cohorts")

    def step_forward(self):
        """Simulate one time step in the projection"""
        super().step_forward()
        if self.current_ts < self.policy_ts:
            predictor = 'before'
        else:
            predictor = 'after'
        outflow_dict = self.admissions_predictors[predictor].get_time_step_estimate(self.current_ts)

        for edge in self.edges:
            edge.ingest_incoming_cohort(outflow_dict)

    def gen_arima_output_df(self, state: str = 'before'):
        return pd.concat({self.tag: self.admissions_predictors[state].gen_arima_output_df()}, names=['compartment'])

    def reallocate_outflow(self, reallocation_fraction: float, outflow: str, new_outflow: str):
        """
        reallocate `reallocation_fraction` of outflows from `outflow` to `new_outflow`
            e.g. if reallocation_fraction is 0.2 and an outflow is 100 in a given ts, it will become 80 to that outflow
            20 added to `new_outflow`
        """
        # pylint: disable=unused-argument

        # generate new outflows
        # new_outflow_data = self.after_data.loc[self.after_data.outflow_to == outflow]
        # new_outflow_data.loc[:, new_outflow_data.columns != 'outflow_to'] *= reallocation_fraction
        # new_outflow_data.loc[:, 'outflow_to'] = new_outflow
        #
        # # scale down old outflows
        # self.after_data.loc[self.after_data.outflow_to == outflow, self.after_data.columns != 'outflow_to'] *= \
        #     1 - reallocation_fraction
        #
        # self.after_data = self.after_data.append(new_outflow_data)

        return
        # for outflow_compartment, row in self.after_data.iterrows():
        #     if outflow_ocmp
