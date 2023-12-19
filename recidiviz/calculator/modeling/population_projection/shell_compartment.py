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
    MIN_POSSIBLE_POLICY_TIME_STEP,
)


class ShellCompartment(SparkCompartment):
    """Simple Spark Compartment that only sends groups to other compartments and does not ingest cohorts"""

    def __init__(
        self,
        outflows_data: pd.DataFrame,
        starting_time_step: int,
        tag: str,
        policy_list: List[SparkPolicy],
        constant_admissions: bool,
    ) -> None:
        super().__init__(outflows_data, starting_time_step, tag)

        self.policy_list = policy_list

        self.admissions_predictors: Dict[int, PredictedAdmissions] = {}

        self.policy_data: Dict[int, pd.DataFrame] = {}

        self._initialize_admissions_predictors(constant_admissions)

    def _initialize_admissions_predictors(self, constant_admissions: bool) -> None:
        """Generate the dictionary of one admission predictor per policy time step that defines admissions behaviors"""
        policy_time_steps = list(
            {policy.policy_time_step for policy in self.policy_list}
        )

        if (
            len(policy_time_steps) > 0
            and min(policy_time_steps) <= MIN_POSSIBLE_POLICY_TIME_STEP
        ):
            raise ValueError(
                f"Policy time_step exceeds minimum allowable value ({MIN_POSSIBLE_POLICY_TIME_STEP}): \
                {min(policy_time_steps)}"
            )

        # TODO(#6727): inheritance -> composition for SparkCompartment so this reused logic is cleaned up

        policy_time_steps.append(MIN_POSSIBLE_POLICY_TIME_STEP)
        policy_time_steps.sort()

        self.policy_data[MIN_POSSIBLE_POLICY_TIME_STEP] = self.historical_outflows

        # first pass transforms admissions data according to policies
        for time_step_idx in range(1, len(policy_time_steps)):
            # start with a copy of the data from the previous policy data
            time_step_data = self.policy_data[
                policy_time_steps[time_step_idx - 1]
            ].copy()

            for policy in SparkPolicy.get_time_step_policies(
                self.policy_list, policy_time_steps[time_step_idx]
            ):
                # TODO(#10442): Fix this and remove the type ignore.
                time_step_data = policy.policy_fn(time_step_data)

            self.policy_data[policy_time_steps[time_step_idx]] = time_step_data

        # second pass creates admissions predictors from transformed outflows data
        for time_step, time_step_data in self.policy_data.items():
            self.admissions_predictors[time_step] = PredictedAdmissions(
                time_step_data, constant_admissions
            )

    def initialize_edges(self, edges: List[SparkCompartment]) -> None:
        """checks all compartments this outflows to are in `self.edges`"""
        edge_tags = [i.tag for i in edges]
        if not set(self.historical_outflows.index).issubset(edge_tags):
            raise ValueError(
                f"Some admission_to compartments are not included in the compartment architecture:\n"
                f"{set(self.historical_outflows.index).difference(edge_tags)}"
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
        policy_time_steps = [
            ts for ts in self.policy_data if ts <= self.current_time_step
        ]
        policy_time_steps.sort()
        outflow_dict = self.admissions_predictors[
            policy_time_steps[-1]
        ].get_time_step_estimate(self.current_time_step)

        # Store the outflows
        self.outflows.loc[:, self.current_time_step] = outflow_dict

        for edge in self.edges:
            edge.ingest_incoming_cohort(outflow_dict)

    def gen_arima_output_df(
        self, time_step: int = MIN_POSSIBLE_POLICY_TIME_STEP
    ) -> pd.DataFrame:
        return pd.concat(
            {self.tag: self.admissions_predictors[time_step].gen_arima_output_df()},
            names=["compartment"],
        )

    @staticmethod
    def reallocate_outflows(
        outflows_data: pd.DataFrame,
        reallocation_fraction: float,
        outflow: str,
        new_outflow: Optional[str] = None,
    ) -> pd.DataFrame:
        raise RuntimeError(
            "This method has been deprecated, use `reallocate_admissions` instead"
        )

    @staticmethod
    def reallocate_admissions(
        admissions_data: pd.DataFrame,
        reallocation_fraction: float,
        admission_to: str,
        new_admission_to: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        reallocate `reallocation_fraction` of admissions from `admission_to` to `new_admission_to` (just scales down
            if no new_admission_to given)
        e.g. if reallocation_fraction is 0.2 and an admission is 100 in a given time-step, it will become 80 to that
            admission 20 added to `new_admission_to`
        """

        after_data = admissions_data.copy()
        if new_admission_to is not None:
            # generate new admissions
            if new_admission_to in after_data.index:
                after_data.loc[new_admission_to] += (
                    after_data.loc[admission_to] * reallocation_fraction
                )
            else:
                after_data.loc[new_admission_to] = (
                    after_data.loc[admission_to] * reallocation_fraction
                )

        # scale down old admissions
        after_data.loc[admission_to] *= 1 - reallocation_fraction

        return after_data

    @staticmethod
    def use_alternate_outflows_data(
        _: pd.DataFrame, alternate_outflows_data: pd.DataFrame, tag: str
    ) -> pd.DataFrame:
        raise RuntimeError(
            "This method has been deprecated, use `use_alternate_admissions_data` instead"
        )

    @staticmethod
    def use_alternate_admissions_data(
        _: pd.DataFrame, alternate_admissions_data: pd.DataFrame, tag: str
    ) -> pd.DataFrame:
        """Swap in entirely different admissions data for 'after' ARIMA fit."""
        # get counts of population from historical data aggregated by compartment, outflow, and year
        preprocessed_data = alternate_admissions_data.groupby(
            ["compartment", "admission_to", "time_step"]
        )["cohort_population"].sum()
        # shadows logic in SubSimulationFactory._load_data()
        after_data = (
            preprocessed_data.unstack(level=["admission_to", "time_step"])
            .stack(level="admission_to", dropna=False)
            .loc[tag]
            .fillna(0)
        )
        return after_data
