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
"""FullCompartment-specific table containing probabilities of transition to other FullCompartments"""

import copy
from typing import Dict, List

import pandas as pd

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)
from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    MIN_POSSIBLE_POLICY_TS,
)


class CompartmentTransitions:
    """Handle transition tables for one compartment that sends groups to multiple other compartments over time"""

    def __init__(self, historical_outflows: pd.DataFrame) -> None:

        self._check_inputs_valid(historical_outflows)

        self.outflows = historical_outflows["outflow_to"].unique()

        self.historical_outflows = historical_outflows

        self.transition_tables: Dict[int, TransitionTable] = {}

    @staticmethod
    def _check_inputs_valid(historical_outflows: pd.DataFrame) -> None:
        """Check historical data passed to CompartmentTransitions is valid."""
        required_columns = [
            "outflow_to",
            "compartment",
            "compartment_duration",
            "total_population",
        ]
        missing_columns = [
            col for col in required_columns if col not in historical_outflows.columns
        ]
        if len(missing_columns) != 0:
            raise ValueError(
                f"historical_outflows dataframe is missing the required columns {required_columns}"
            )

        if "remaining" in historical_outflows["outflow_to"].unique():
            raise ValueError(
                "historical_outflows dataframe cannot contain an outflow column named `remaining`"
            )

        if historical_outflows.empty:
            raise ValueError(
                "Cannot create a transition table with an empty transitions_data dataframe"
            )

        for column in ["total_population", "compartment_duration"]:
            if any(historical_outflows[column] < 0):
                negative_rows = historical_outflows[historical_outflows[column] < 0]
                raise ValueError(
                    f"Transition data '{column}' column cannot contain negative values {negative_rows}"
                )
            if any(historical_outflows[column].isnull()):
                null_rows = historical_outflows[historical_outflows[column].isnull()]
                raise ValueError(
                    f"Transition data '{column}' column cannot contain NULL values {null_rows}"
                )

    def initialize_transition_tables(self, policy_list: List[SparkPolicy]) -> None:
        """Populate the 'before' transition table and initializes the max_sentence from historical data """
        self.transition_tables[MIN_POSSIBLE_POLICY_TS] = TransitionTable(
            MIN_POSSIBLE_POLICY_TS, []
        )
        self.transition_tables[MIN_POSSIBLE_POLICY_TS].generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TS], self.historical_outflows
        )

        policy_time_steps = sorted({policy.policy_ts for policy in policy_list})

        if (
            len(policy_time_steps) > 0
            and min(policy_time_steps) <= MIN_POSSIBLE_POLICY_TS
        ):
            raise ValueError(
                f"Policy ts exceeds minimum allowable value ({MIN_POSSIBLE_POLICY_TS}): {min(policy_time_steps)}"
            )

        policy_time_steps.append(MIN_POSSIBLE_POLICY_TS)
        policy_time_steps.sort()

        for tsi, ts in enumerate(policy_time_steps[1:], 1):
            prev_tables = copy.deepcopy(
                self.transition_tables[policy_time_steps[tsi - 1]].tables
            )
            self.transition_tables[ts] = TransitionTable(
                ts, SparkPolicy.get_ts_policies(policy_list, ts), prev_tables
            )

        # normalize all tables
        for transition_table in self.transition_tables.values():
            transition_table.normalize_transitions()

    def get_per_ts_transition_table(self, current_ts: int) -> pd.DataFrame:
        """function used by SparkCompartment to determine which of the state transition tables to pull from"""

        policy_time_steps = [ts for ts in self.transition_tables if ts <= current_ts]
        policy_time_steps.sort(reverse=True)
        # take transitions from the most recent table whose policy ts has already passed
        return self.transition_tables[policy_time_steps[0]].get_per_ts_table(current_ts)
