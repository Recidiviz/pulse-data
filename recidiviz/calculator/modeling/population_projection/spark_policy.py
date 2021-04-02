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

from typing import Any, Callable, Dict, List


class SparkPolicy:
    """Policy function encapsulation"""

    def __init__(
        self,
        policy_fn: Callable,
        spark_compartment: str,
        sub_population: Dict[str, str],
        policy_ts: int,
        apply_retroactive: bool = False,
    ):
        """
        Create an object to store the policy metadata including the area where the policy should be applied

        `policy_fn` the method to use for the transition object in order to simulate the policy change
        `spark_compartment` the name of the compartment where this policy applies
        `sub_population` the dictionary of disaggregated axes where this policy applies
        `policy_ts` the time step at which the policy occurs
        `apply_retroactive` True if the policy should be applied retroactively to those already in the system
        """

        self.policy_fn = policy_fn
        self.spark_compartment = spark_compartment
        self.sub_population = sub_population
        self.policy_ts = policy_ts
        self.apply_retroactive = apply_retroactive

    @staticmethod
    def _get_applicable_policies(
        policy_list: List, population_type: str, population_key: Any
    ):
        """Return a list of SparkPolicy objects relevant to the specific sub population or compartment.

        `policy_list` List of SparkPolicy objects
        `population_type` str attribute from the SparkPolicy class such as `spark_compartment` or `sub_population`
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
    def get_compartment_policies(policy_list: List, spark_compartment: str):
        """Return a list of SparkPolicy objects for the specific compartment."""
        return SparkPolicy._get_applicable_policies(
            policy_list, "spark_compartment", spark_compartment
        )

    @staticmethod
    def get_sub_population_policies(policy_list: List, sub_population: Dict[str, str]):
        """Return a list of SparkPolicy objects for the specific sub_population."""
        return SparkPolicy._get_applicable_policies(
            policy_list, "sub_population", sub_population
        )

    @staticmethod
    def get_ts_policies(policy_list: List["SparkPolicy"], time_step: int):
        """Return a list of SParkPolicy objects for the specific time step."""
        return SparkPolicy._get_applicable_policies(policy_list, "policy_ts", time_step)
