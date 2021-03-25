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
"""Object representing a location in the justice system"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from copy import deepcopy
import pandas as pd


class SparkCompartment(ABC):
    """Encapsulate all the logic for one compartment within the simulation"""

    def __init__(
        self, outflows_data: pd.DataFrame, starting_ts: int, policy_ts: int, tag: str
    ):
        # the first ts of the projection
        self.ts_start = starting_ts

        # the ts the policies are implemented
        self.policy_ts = policy_ts

        # the current ts of the simulation
        self.current_ts = starting_ts

        # historical data of outflow from compartment
        self.outflows_data = outflows_data

        # SparkCompartments this compartment feeds to
        self.edges: Optional[List[SparkCompartment]] = None

        # key used for this compartment in outflow dicts
        self.tag = tag

        # validation features
        self.error = pd.DataFrame(
            0, index=outflows_data.index, columns=outflows_data.columns
        )
        self.outflows = pd.DataFrame(index=outflows_data.index)

    def initialize_edges(self, edges: List):
        self.edges = edges

    @abstractmethod
    def step_forward(self):
        """Simulate one time step in the projection"""
        if self.edges is None:
            raise ValueError(
                f"Compartment {self.tag} needs initialized edges before running the simulation"
            )

    @abstractmethod
    def ingest_incoming_cohort(self, influx: Dict[str, float]):
        """Ingest the population coming from one compartment into another by the end of the `current_ts`

        influx: dictionary of cohort type (str) to number of people revoked for the time period (int)
        """

    def prepare_for_next_step(self):
        """Clean up any data structures and move the time step 1 unit forward"""
        # increase the `current_ts` by 1 to simulate the population at the beginning of the next ts
        self.current_ts += 1

    def get_error(self, unit="abs"):
        if unit == "abs":
            return self.error.sort_index(axis=1).transpose()
        if unit == "mse":
            mse_error = deepcopy(self.error)
            for outflow in mse_error:
                mse_error[outflow] = (
                    mse_error[outflow] / 100
                ) ** 2 * self.outflows_data[outflow]
            return mse_error.sort_index(axis=1).transpose()

        raise RuntimeError("unrecognized unit")
