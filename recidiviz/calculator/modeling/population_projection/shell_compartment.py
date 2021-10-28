# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

from typing import Dict, List, Optional

import pandas as pd

from recidiviz.calculator.modeling.population_projection.predicted_admissions import (
    PredictedAdmissions,
)
from recidiviz.calculator.modeling.population_projection.spark_compartment import (
    SparkCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    MIN_POSSIBLE_POLICY_TS,
)


class ShellCompartment(SparkCompartment):
    """Simple Spark Compartment that only sends groups to other compartments and does not ingest cohorts"""

    def __init__(
        self,
        outflows_data: pd.DataFrame,
        starting_ts: int,
        tag: str,
        policy_list: List[SparkPolicy],
        constant_admissions: bool,
    ) -> None:

        super().__init__(outflows_data, starting_ts, tag)

        self.policy_list = policy_list

        self.admissions_predictors: Dict[int, PredictedAdmissions] = {}

        self.policy_data: Dict[int, pd.DataFrame] = {}

        self._initialize_admissions_predictors(constant_admissions)

    def _initialize_admissions_predictors(self, constant_admissions: bool) -> None:
        """Generate the dictionary of one admission predictor per policy time step that defines outflow behaviors"""
        policy_time_steps = list({policy.policy_ts for policy in self.policy_list})

        if (
            len(policy_time_steps) > 0
            and min(policy_time_steps) <= MIN_POSSIBLE_POLICY_TS
        ):
            raise ValueError(
                f"Policy ts exceeds minimum allowable value ({MIN_POSSIBLE_POLICY_TS}): {min(policy_time_steps)}"
            )

        # TODO(#6727): inheritance -> composition for SparkCompartment so this reused logic is cleaned up

        policy_time_steps.append(MIN_POSSIBLE_POLICY_TS)
        policy_time_steps.sort()

        self.policy_data[MIN_POSSIBLE_POLICY_TS] = self.outflows_data

        # first pass transforms outflows data according to policies
        for ts_idx in range(1, len(policy_time_steps)):
            # start with a copy of the data from the previous policy data
            ts_data = self.policy_data[policy_time_steps[ts_idx - 1]].copy()

            for policy in SparkPolicy.get_ts_policies(
                self.policy_list, policy_time_steps[ts_idx]
            ):
                ts_data = policy.policy_fn(ts_data)

            self.policy_data[policy_time_steps[ts_idx]] = ts_data

        # second pass creates admissions predictors from transformed outflows data
        for ts, ts_data in self.policy_data.items():
            self.admissions_predictors[ts] = PredictedAdmissions(
                ts_data, constant_admissions
            )

    def initialize_edges(self, edges: List[SparkCompartment]) -> None:
        """checks all compartments this outflows to are in `self.edges`"""
        edge_tags = [i.tag for i in edges]
        if not set(self.outflows_data.index).issubset(edge_tags):
            raise ValueError(
                f"Some edges are not supported in the outflows data"
                f"Expected:{self.outflows_data.index}, Actual: {edge_tags}"
            )
        super().initialize_edges(edges)

    def ingest_incoming_cohort(self, influx: Dict[str, float]) -> None:
        """Ingest the population coming from one compartment into another by the end of the `current ts`

        influx: dictionary of cohort type (str) to number of people revoked for the time period (float)
        """
        if self.tag in influx:
            raise ValueError(f"Shell compartment {self.tag} cannot ingest cohorts")

    def step_forward(self) -> None:
        """Simulate one time step in the projection"""
        super().step_forward()
        policy_time_steps = [ts for ts in self.policy_data if ts <= self.current_ts]
        policy_time_steps.sort()
        outflow_dict = self.admissions_predictors[
            policy_time_steps[-1]
        ].get_time_step_estimate(self.current_ts)

        # Store the outflows
        self.outflows[self.current_ts] = pd.Series(outflow_dict, dtype=float)

        for edge in self.edges:
            edge.ingest_incoming_cohort(outflow_dict)

    def gen_arima_output_df(
        self, time_step: int = MIN_POSSIBLE_POLICY_TS
    ) -> pd.DataFrame:
        return pd.concat(
            {self.tag: self.admissions_predictors[time_step].gen_arima_output_df()},
            names=["compartment"],
        )

    @staticmethod
    def reallocate_outflow(
        outflows_data: pd.DataFrame,
        reallocation_fraction: float,
        outflow: str,
        new_outflow: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        reallocate `reallocation_fraction` of outflows from `outflow` to `new_outflow` (just scales down
            if no new_outflow given)
        e.g. if reallocation_fraction is 0.2 and an outflow is 100 in a given ts, it will become 80 to that outflow
            20 added to `new_outflow`
        """

        after_data = outflows_data.copy()
        if new_outflow is not None:
            # generate new outflows
            if new_outflow in after_data.index:
                after_data.loc[new_outflow] += (
                    after_data.loc[outflow] * reallocation_fraction
                )
            else:
                after_data.loc[new_outflow] = (
                    after_data.loc[outflow] * reallocation_fraction
                )

        # scale down old outflows
        after_data.loc[outflow] *= 1 - reallocation_fraction

        return after_data

    @staticmethod
    def use_alternate_outflows_data(
        _: pd.DataFrame, alternate_outflows_data: pd.DataFrame, tag: str
    ) -> pd.DataFrame:
        """Swap in entirely different outflows data for 'after' ARIMA fit."""
        # get counts of population from historical data aggregated by compartment, outflow, and year
        preprocessed_data = alternate_outflows_data.groupby(
            ["compartment", "outflow_to", "time_step"]
        )["total_population"].sum()
        # shadows logic in SubSimulationFactory._load_data()
        after_data = (
            preprocessed_data.unstack(level=["outflow_to", "time_step"])
            .stack(level="outflow_to", dropna=False)
            .loc[tag]
            .fillna(0)
        )
        return after_data
