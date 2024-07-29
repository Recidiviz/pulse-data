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
"""Object representing a policy to be applied in a simulation"""

from functools import partial
from typing import Any, Callable, List, Optional

import pandas as pd

from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)


class SparkPolicy:
    """Policy function encapsulation"""

    def __init__(
        self,
        # policy_fn can take a transition_table.TransitionTable, modify it in place, then return None
        # e.g. transition_table.py (TransitionTable.reallocate_outflow)
        # policy_fn can also take a pd.DataFrame, without modifying it, and return a new pd.DataFrame
        # e.g. shell_compartment.py (ShellCompartment.use_alternate_outflows_data)
        spark_compartment: str,
        simulation_group: str,
        policy_time_step: int,
        apply_retroactive: bool,
        policy_fn: Callable = TransitionTable.use_alternate_transitions_data,
        alternate_transitions_data: Optional[pd.DataFrame] = None,
    ) -> None:
        """
        Create an object to store the policy metadata including the area where the policy should be applied

        `spark_compartment` the name of the compartment where this policy applies
        `simulation_group` the tag of the subgroup where this policy applies
        `policy_time_step` the time step at which the policy occurs
        `apply_retroactive` True if the policy should be applied retroactively to those already in the system
        `policy_fn` the method to use for the transition object in order to simulate the policy change
        `alternate_transitions_data` the alternate transitions to substitute. Should only be provided if using default
            policy_fn.
        """
        if (
            policy_fn != TransitionTable.use_alternate_transitions_data
        ) and alternate_transitions_data:
            raise ValueError(
                "Cannot pass alternate transitions data if not using default policy function"
            )

        if (policy_fn == TransitionTable.use_alternate_transitions_data) and (
            alternate_transitions_data is None
        ):
            raise ValueError(
                "No alternate transitions data passed in to substitute in policy scenario"
            )

        self.spark_compartment = spark_compartment
        self.simulation_group = simulation_group
        self.policy_time_step = policy_time_step
        self.apply_retroactive = apply_retroactive
        self.policy_fn = self._build_policy_function(
            policy_fn, apply_retroactive, alternate_transitions_data
        )
        self.alternate_transitions_data = alternate_transitions_data

    @staticmethod
    def _build_policy_function(
        policy_fn: Callable,
        apply_retroactive: bool,
        alternate_transitions_data: pd.DataFrame,
    ) -> Callable:
        """Helper function to initialize policy function"""
        if policy_fn != TransitionTable.use_alternate_transitions_data:
            return policy_fn

        return partial(
            policy_fn,
            alternate_historical_transitions=alternate_transitions_data,
            retroactive=apply_retroactive,
        )

    @staticmethod
    def _get_applicable_policies(
        policy_list: List["SparkPolicy"], population_type: str, population_key: Any
    ) -> List["SparkPolicy"]:
        """Return a list of SparkPolicy objects relevant to the specific sub population or compartment.

        `policy_list` List of SparkPolicy objects
        `population_type` str attribute from the SparkPolicy class such as `spark_compartment` or `simulation_group`
        `population_key` the identifier used to match the specific sub population or compartment
        """
        # Select all values where the policy attribute matches the provided key
        policy_subset = [
            policy
            for policy in policy_list
            if getattr(policy, population_type) == population_key
        ]
        return policy_subset

    @staticmethod
    def get_compartment_policies(
        policy_list: List["SparkPolicy"], spark_compartment: str
    ) -> List["SparkPolicy"]:
        """Return a list of SparkPolicy objects for the specific compartment."""
        return SparkPolicy._get_applicable_policies(
            policy_list, "spark_compartment", spark_compartment
        )

    @staticmethod
    def get_sub_population_policies(
        policy_list: List["SparkPolicy"], simulation_group: str
    ) -> List["SparkPolicy"]:
        """Return a list of SparkPolicy objects for the specific simulation_group."""
        return SparkPolicy._get_applicable_policies(
            policy_list, "simulation_group", simulation_group
        )

    @staticmethod
    def get_time_step_policies(
        policy_list: List["SparkPolicy"], time_step: int
    ) -> List["SparkPolicy"]:
        """Return a list of SParkPolicy objects for the specific time step."""
        return SparkPolicy._get_applicable_policies(
            policy_list, "policy_time_step", time_step
        )
