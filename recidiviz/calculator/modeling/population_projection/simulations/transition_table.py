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
"""table containing probabilities of transition to other FullCompartments used by CompartmentTransitions object"""
from copy import deepcopy
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    SIG_FIGS,
    TransitionTableType,
)


class TransitionTable:
    """Handle transitions around one policy time_step for population projection modeling"""

    def __init__(
        self,
        policy_ts: int,
        policy_list: List[SparkPolicy],
        previous_table: Optional[pd.DataFrame] = None,
    ):
        """
        policy_ts: ts at which the related policy is enacted
        policy_list: list of SparkPolicy that apply on policy_ts
        max_sentence: length of transition tables
        previous_table: preceding TransitionTable. Should only be None for chronologically first table
        """
        self.previous_table = previous_table
        self.policy_ts = policy_ts
        self.policy_list = policy_list

        if previous_table is not None:
            self.max_sentence = int(np.ceil(previous_table.index.max()))
        else:
            self.max_sentence = 0

        self.transition_dfs: Dict[TransitionTableType, pd.DataFrame] = {
            TransitionTableType.BEFORE: pd.DataFrame(),
            TransitionTableType.AFTER: pd.DataFrame(),
            TransitionTableType.TRANSITORY: pd.DataFrame(),
        }

        self._initialize_tables()

    def _initialize_tables(self) -> None:
        """Generate `before`, `transitory`, and `after` transition tables."""

        # if chronologically first table, no policies and only 'after' needs to be populated, which is handled elsewhere
        if self.previous_table is None:
            return

        if "remaining" in self.previous_table:
            raise ValueError(
                "Cannot create a transition table from a normalized previous table"
            )

        # Apply all the non-retroactive policy functions to the 'before' table
        self.transition_dfs[TransitionTableType.BEFORE] = self.previous_table.copy()
        for policy in self.policy_list:
            if policy.apply_retroactive:
                policy.policy_fn(self)

        before_table_invariance_under_non_retroactive_policies_checker = (
            self.transition_dfs[TransitionTableType.BEFORE].copy()
        )

        self.transition_dfs[TransitionTableType.AFTER] = self.transition_dfs[
            TransitionTableType.BEFORE
        ].copy()
        # Add on the retroactive policy functions to the 'after' table
        for policy in self.policy_list:
            if not policy.apply_retroactive:
                policy.policy_fn(self)

        # make sure policy affects correct transition table
        if any(
            list(before_table_invariance_under_non_retroactive_policies_checker[i])
            != list(self.transition_dfs[TransitionTableType.BEFORE][i])[
                : len(before_table_invariance_under_non_retroactive_policies_checker[i])
            ]
            for i in self.transition_dfs[TransitionTableType.BEFORE]
        ):
            raise ValueError(
                "Policy function was applied to the wrong transition state"
            )

        self.transition_dfs[TransitionTableType.TRANSITORY] = self.transition_dfs[
            TransitionTableType.BEFORE
        ].copy()

    def _normalize_table(
        self, state: TransitionTableType, before_table: Optional[pd.DataFrame] = None
    ) -> None:
        """Convert the per-ts population counts into normalized probabilities"""
        # if first table, skip before and transitory states
        if self.previous_table is None and state in [
            TransitionTableType.BEFORE,
            TransitionTableType.TRANSITORY,
        ]:
            return

        if state == TransitionTableType.PREVIOUS:
            after_table = self.previous_table
        else:
            after_table = self.transition_dfs[state]

        self._check_table_exists(state)

        # Just here to satisfy mypy
        if after_table is None:
            raise ValueError

        if after_table.empty:
            raise ValueError(
                "Cannot normalize transition tables before they are initialized"
            )

        if "remaining" in after_table:
            raise ValueError(
                f"Trying to normalize a transition table that is already normalized: {after_table}"
            )

        if before_table is None:
            before_table = after_table

        if "remaining" in before_table:
            raise ValueError(
                "`normalize_transitions` cannot be called with a normalized `before_table`"
            )

        normalized_df = deepcopy(after_table)

        before_counted = 0
        after_counted = 0

        before_total = before_table.sum().sum()
        after_total = after_table.sum().sum()

        outflow_weights = before_table.sum() / before_total

        for sentence_len in range(1, self.max_sentence + 1):

            if (
                (sentence_len > before_table.index.max())
                or (before_counted == before_total)
                or (after_counted == after_total)
            ):
                normalized_df.loc[sentence_len] = outflow_weights
                continue

            ts_released = after_table.loc[sentence_len].sum()

            after_counted += ts_released

            after_remaining = 1 - after_counted / after_total
            before_remaining = 1 - before_counted / before_total

            # if more people released by this point after than before, this will be negative but we actually want it to
            #   be 0
            ts_release_rate = max(1 - after_remaining / before_remaining, 0)

            if ts_released > 0:
                normalized_df.loc[sentence_len] *= ts_release_rate / ts_released
            else:
                normalized_df.loc[sentence_len] = ts_release_rate * outflow_weights

            before_counted += before_table.loc[sentence_len].sum()

        normalized_df = normalized_df.apply(lambda x: round(x, SIG_FIGS))

        # Assign the residual probability as the proportion that remains in the current compartment per month
        normalized_df["remaining"] = 1 - round(normalized_df.sum(axis=1), SIG_FIGS - 1)

        # Check that the transition table is valid
        full_release_times = np.isclose(normalized_df["remaining"], 0, SIG_FIGS)
        if full_release_times.sum() == 0:
            raise ValueError(
                f"Transition table doesn't release everyone: "
                f"{normalized_df.iloc[-1]}"
            )

        # Make sure all transition probabilities are between 0-1
        for compartment, transition_df in normalized_df.items():
            if any((transition_df < 0) | (transition_df > 1)):
                erroneous_values = transition_df[
                    (transition_df < 0) | (transition_df > 1)
                ]
                raise ValueError(
                    f"'{compartment}' transition has probabilities out of bounds for state '{state}':\n"
                    f"{erroneous_values}"
                )

        max_sentence = min(
            self.max_sentence, min(normalized_df[full_release_times].index)
        )
        normalized_df.loc[max_sentence, "remaining"] = 0

        if state == TransitionTableType.PREVIOUS:
            self.previous_table = normalized_df
        else:
            self.transition_dfs[state] = normalized_df

    def normalize_transitions(self) -> None:
        """standardize the transition probabilities for all transition tables"""
        self._normalize_table(
            TransitionTableType.TRANSITORY, before_table=self.previous_table
        )
        self._normalize_table(TransitionTableType.BEFORE)
        self._normalize_table(TransitionTableType.AFTER)

    def generate_transition_table(
        self, state: TransitionTableType, historical_outflows: pd.DataFrame
    ) -> None:
        """
        Accepts a state for which to populate transition probabilities
        Accepts a DataFrame with transition data to use
        Should only be called for chronologically first TransitionTable in a baseline simulation
        """
        sentence_length = historical_outflows["compartment_duration"].apply(np.ceil)

        grouped_outflows = historical_outflows.groupby(
            [sentence_length, historical_outflows.outflow_to]
        )

        self.max_sentence = max(self.max_sentence, int(sentence_length.max()))

        self.transition_dfs[state] = (
            grouped_outflows["total_population"]
            .sum()
            .unstack()
            .reindex(range(1, self.max_sentence + 1))
            .fillna(0)
        )

    def get_table(self, state: TransitionTableType) -> pd.DataFrame:
        """Return a transition_df"""
        if state == TransitionTableType.PREVIOUS:
            return self.previous_table
        return self.transition_dfs[state]

    def get_per_ts_table(self, current_ts: int) -> pd.DataFrame:
        """Returns a combination of transition_dfs for the given ts"""

        if self.previous_table is None:
            populated_states = [TransitionTableType.AFTER]
        else:
            populated_states = list(self.transition_dfs.keys())
        for state in populated_states:
            if "remaining" not in self.transition_dfs[state].columns:
                raise ValueError(
                    f"Transition table for the '{state}' has not been normalized"
                )

        if current_ts < self.policy_ts:
            raise ValueError(
                f"Trying to use transition table for policy_ts {self.policy_ts} too early (current_ts: {current_ts})"
            )

        # first ts of policy, need to transition from one table to the other
        if current_ts == self.policy_ts:
            return self.transition_dfs[TransitionTableType.TRANSITORY]

        # post policy implementation
        ts_since_policy = current_ts - self.policy_ts
        if ts_since_policy >= self.max_sentence:
            return self.transition_dfs[TransitionTableType.AFTER]
        # from 'after' table, take indices corresponding to LOS in compartment up until those as old as the policy
        #   then take those with LOS longer than years since policy from 'before' table
        return pd.concat(
            [
                self.transition_dfs[TransitionTableType.AFTER].loc[:ts_since_policy, :],
                self.transition_dfs[TransitionTableType.BEFORE].loc[
                    ts_since_policy + 1 :, :
                ],
            ]
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, TransitionTable):
            return False
        for state in self.transition_dfs:
            if set(self.transition_dfs[state].columns) != set(
                other.transition_dfs[state].columns
            ):
                return False
            matching_order_self = self.transition_dfs[state][
                other.transition_dfs[state].columns
            ]
            if (matching_order_self != other.transition_dfs[state]).all().all():
                return False
        return True

    def _check_table_invariant_before_normalization(
        self, state: TransitionTableType
    ) -> None:
        """Raise an exception if the underlying transition table has been normalized before applying a policy"""
        self._check_table_exists(state)
        if state == TransitionTableType.PREVIOUS:
            table = self.previous_table
        else:
            table = self.transition_dfs[state]
        if (table is None) or "remaining" in table:
            raise ValueError(
                "Policy method cannot be applied after the transition table is normalized"
            )

    def _check_table_exists(self, state: TransitionTableType) -> None:
        """Make sure table is not null"""
        if state == TransitionTableType.PREVIOUS:
            table = self.previous_table
        else:
            table = self.transition_dfs[state]

        if table is None:
            raise ValueError(
                f"Cannot normalize {state} table for first TransitionTable of CompartmentTransitions object"
            )

    @staticmethod
    def get_df_name_from_retroactive(retroactive: bool) -> TransitionTableType:
        if retroactive:
            return TransitionTableType.BEFORE

        return TransitionTableType.AFTER

    def test_non_retroactive_policy(self) -> None:
        self.transition_dfs[TransitionTableType.AFTER]["jail"] = 0

    def test_retroactive_policy(self) -> None:
        self.transition_dfs[TransitionTableType.BEFORE]["jail"] = 0

    def extend_tables(self, new_max_sentence: int) -> None:
        """
        extends the max sentence of transition tables to `new_max_sentence` --> doesn't handle long-sentences
            correctly but those should get taken out anyway
        doesn't shorten max sentence if new_max_sentence is smaller than self.max_sentence
        """
        # Ensure none of the transition tables have been normalized before they are extended
        for state in self.transition_dfs:
            if not self.transition_dfs[state].empty:
                self._check_table_invariant_before_normalization(state)

        if new_max_sentence <= self.max_sentence:
            return

        for state in self.transition_dfs:
            # Extend the populated transition tables
            if not self.transition_dfs[state].empty:
                extended_index = pd.Index(
                    data=range(self.max_sentence + 1, new_max_sentence + 1),
                    name=self.transition_dfs[state].index.name,
                )
                extended_df = pd.DataFrame(
                    index=extended_index,
                    columns=self.transition_dfs[state].columns,
                )
                self.transition_dfs[state] = (
                    self.transition_dfs[state].append(extended_df).fillna(0)
                )

        self.max_sentence = new_max_sentence

    def unnormalize_table(self, state: TransitionTableType) -> None:
        """revert a normalized table back to an un-normalized df. sum of all total populations will be 1"""
        if state == TransitionTableType.PREVIOUS:
            table = self.previous_table
        else:
            table = self.transition_dfs[state]

        self._check_table_exists(state)
        # Just here to satisfy mypy
        if table is None:
            raise ValueError

        if "remaining" not in table:
            raise ValueError("trying to unnormalize a table that isn't normalized")

        for sentence_length in table.index.sort_values(ascending=False):
            for shorter_sentence in range(1, sentence_length):
                table.loc[sentence_length] *= (
                    1 - table.loc[shorter_sentence].drop("remaining").sum()
                )

        table = table.drop("remaining", axis=1)

        if state == TransitionTableType.PREVIOUS:
            self.previous_table = table
        else:
            self.transition_dfs[state] = table

    def use_alternate_transitions_data(
        self, alternate_historical_transitions: pd.DataFrame, retroactive: bool
    ) -> None:
        """Replace the historical admission data for this specific group with another data from a different set"""

        self.extend_tables(
            int(alternate_historical_transitions.compartment_duration.max())
        )
        df_name = self.get_df_name_from_retroactive(retroactive)
        self.generate_transition_table(df_name, alternate_historical_transitions)

    def preserve_normalized_outflow_behavior(
        self,
        outflows: List[str],
        state: TransitionTableType,
        before_state: TransitionTableType = TransitionTableType.PREVIOUS,
    ) -> None:
        """
        change the transition probabilities for outflows so the yearly (normalized) percentage of those outflows per
            year match 'before' state
        `outflows` should be a list of outflows to affect
        `state` should be the transition table for which to affect them
        `before_state` should be the transition table to copy normalized behavior from
        """
        self._check_table_exists(state)
        self._check_table_invariant_before_normalization(state)

        if state == before_state:
            raise ValueError("matching state to itself")

        self._normalize_table(before_state)
        self._normalize_table(state)

        if before_state == TransitionTableType.PREVIOUS:
            before_table = self.previous_table
        else:
            before_table = self.transition_dfs[before_state]

        # Just here to satisfy mypy
        if before_table is None:
            raise ValueError

        if self.max_sentence <= 0:
            raise ValueError("Max sentence length is not set")

        old_total_outflows = (
            self.transition_dfs[state].drop("remaining", axis=1).sum(axis=1)
        )
        old_total_remaining = (1 - old_total_outflows).cumprod()

        # set normalized probabilities equal to 'before' for desired outflows
        self.transition_dfs[state].loc[:, outflows] = before_table.loc[:, outflows]

        new_total_outflows = (
            self.transition_dfs[state].drop("remaining", axis=1).sum(axis=1)
        )
        # will change as we go, so can't cumprod all at once
        new_total_remaining = 1 - new_total_outflows

        # re-normalize other outflows
        for sentence_length in range(2, self.max_sentence + 1):
            # if no remaining, we're done
            #   only need to check one of the two because scaling will never bring outflows from non-zero to zero
            if new_total_remaining[sentence_length] == 0:
                break

            # cumprod happens here, step by step as the df is updated
            new_total_remaining[sentence_length] *= new_total_remaining[
                sentence_length - 1
            ]

            # re-normalize un-affected outflows for this row
            self.transition_dfs[state].loc[
                sentence_length, ~self.transition_dfs[state].columns.isin(outflows)
            ] *= (
                old_total_remaining[sentence_length - 1]
                / new_total_remaining[sentence_length - 1]
            )

        # revert altered table back to un-normalized
        self.unnormalize_table(before_state)
        self.unnormalize_table(state)

    def apply_reduction(
        self, reduction_df: pd.DataFrame, reduction_type: str, retroactive: bool = False
    ) -> None:
        """
        scale down outflow compartment_duration distributions either multiplicatively or additively
        NOTE: does not change other outflows, ie their normalized values will be different!
        `reduction_df` should be a df with the following columns
            'outflow': str --> affected outflow
            'affected_fraction': float --> fraction of outflow to shift
            'reduction_size': float'
                if `reduction_type` = '*', units is fractional reduction in compartment durations
                    (0.2 --> 10 year sentence becomes 8 years)
                if `reduction_type` = '+', units of time steps
                    (0.5 --> 10 year sentence becomes 9.5 years)
        """
        # ensure tables haven't been normalized already

        if self.max_sentence <= 0:
            raise ValueError("Max sentence length is not set")

        df_name = self.get_df_name_from_retroactive(retroactive)

        self._check_table_invariant_before_normalization(df_name)

        for _, row in reduction_df.iterrows():
            for sentence_length in range(1, self.max_sentence + 1):
                # record population to re-distribute
                sentence_count = (
                    self.transition_dfs[df_name].loc[sentence_length, row.outflow]
                    * row.affected_fraction
                )

                # start by clearing df entry that's getting re-distributed
                self.transition_dfs[df_name].loc[sentence_length, row.outflow] *= (
                    1 - row.affected_fraction
                )

                # calculate new sentence length
                if reduction_type == "*":
                    new_sentence_length = max(
                        [sentence_length * (1 - row.reduction_size), 1]
                    )
                elif reduction_type == "+":
                    new_sentence_length = max([sentence_length - row.reduction_size, 1])
                else:
                    raise RuntimeError(
                        f"reduction type {reduction_type} not recognized (must be '*' or '+')"
                    )

                # separate out non-integer sentence length into one chunk rounded up and one chunk rounded down,
                #   weighted by where in the middle actual sentence falls
                longer_bit = (new_sentence_length % 1) * sentence_count
                shorter_bit = sentence_count - longer_bit

                # add in new sentence length probabilities to the df
                self.transition_dfs[df_name].loc[
                    int(new_sentence_length), row.outflow
                ] += shorter_bit
                if longer_bit > 0:
                    self.transition_dfs[df_name].loc[
                        int(new_sentence_length) + 1, row.outflow
                    ] += longer_bit

    def reallocate_outflow(
        self,
        reallocation_df: pd.DataFrame,
        reallocation_type: str,
        retroactive: bool = False,
    ) -> None:
        """
        reallocation_df should be a df with columns
            'outflow': outflow to be reallocated
            'affected_fraction' : 0 =< float =< 1
            'new_outflow': outflow_tag
        reallocation_type: '*' or '+' --> if '*', scale new_outflow. if '+', add original sentence distribution
        If `new_outflow` doesn't exist, create a new column for it. If null, just scale down (BE VERY USING NULL,
            EASY TO MESS UP NORMALIZATION)
        """
        df_name = self.get_df_name_from_retroactive(retroactive)
        # ensure tables haven't been normalized already
        self._check_table_invariant_before_normalization(df_name)

        if (
            reallocation_type == "*"
            and not reallocation_df.new_outflow.isin(self.transition_dfs[df_name]).all()
        ):
            raise ValueError(
                "Cannot use scaling methodology if new_outflow not already in transition table"
            )

        for _, row in reallocation_df.iterrows():
            before_outflow = np.array(self.transition_dfs[df_name][row.outflow])
            self.transition_dfs[df_name][row.outflow] = list(
                before_outflow * (1 - row.affected_fraction)
            )

            if not row.isnull().new_outflow:
                if reallocation_type == "+":
                    new_outflow_value = list(
                        np.array(self.transition_dfs[df_name].get(row.new_outflow, 0))
                        + before_outflow * row.affected_fraction
                    )
                    self.transition_dfs[df_name][row.new_outflow] = new_outflow_value
                elif reallocation_type == "*":
                    reallocated_population = sum(before_outflow) * row.affected_fraction
                    new_outflow_population = sum(
                        self.transition_dfs[df_name][row.new_outflow]
                    )
                    scale_factor = 1 + reallocated_population / new_outflow_population
                    updated_new_outflow = (
                        np.array(self.transition_dfs[df_name][row.new_outflow])
                        * scale_factor
                    )
                    self.transition_dfs[df_name][row.new_outflow] = list(
                        updated_new_outflow
                    )

                else:
                    raise RuntimeError(
                        f"reallocation type {reallocation_type} not recognized (must be '*' or '+')"
                    )

    def abolish_mandatory_minimum(
        self,
        historical_outflows: pd.DataFrame,
        current_mm: float,
        outflow: str,
        retroactive: bool = False,
    ) -> None:
        """
        Reduce compartment durations as associated with a mandatory minimum reduction/removal. Our methodology for
            this involves scaling down the entire distribution additively by the product of distribution std dev and
            fraction of population sentenced at the mandatory minimum.
        """
        mm_sentenced_group = historical_outflows[
            historical_outflows.compartment_duration == current_mm
        ]
        # Do not modify the transition table if there are no sentences at the mandatory minimum
        if len(mm_sentenced_group) == 0:
            return

        affected_ratio = (
            mm_sentenced_group["total_population"].sum()
            / historical_outflows["total_population"].sum()
        )

        # calculate standard deviation
        average_duration = np.average(
            historical_outflows.compartment_duration,
            weights=historical_outflows.total_population,
        )
        variance = np.average(
            (historical_outflows.compartment_duration - average_duration) ** 2,
            weights=historical_outflows.total_population,
        )
        std = np.sqrt(variance)

        mm_factor = affected_ratio * std

        self.apply_reduction(
            reduction_df=pd.DataFrame(
                {
                    "outflow": [outflow],
                    "affected_fraction": [1],
                    "reduction_size": [mm_factor],
                }
            ),
            reduction_type="+",
            retroactive=retroactive,
        )

    def chop_technical_revocations(
        self,
        technical_outflow: str,
        release_outflow: str = "release",
        retroactive: bool = False,
    ) -> None:
        """Remove all technical revocations that happen after the latest completion duration."""
        df_name = self.get_df_name_from_retroactive(retroactive)
        technical_transitions = self.transition_dfs[df_name][technical_outflow]
        release_transitions = self.transition_dfs[df_name][release_outflow]

        # get max completion duration
        max_release_duration = release_transitions[release_transitions > 0].index.max()
        chopped_indices = technical_transitions.index > max_release_duration

        # get population to reallocate
        chopped_population = technical_transitions[chopped_indices].sum()

        # chop technicals
        self.transition_dfs[df_name].loc[chopped_indices, technical_outflow] = 0

        # reallocate to release
        self.transition_dfs[df_name].loc[
            max_release_duration, release_outflow
        ] += chopped_population
