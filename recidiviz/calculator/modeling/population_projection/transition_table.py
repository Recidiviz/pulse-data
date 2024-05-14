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
import collections
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    SIG_FIGS,
)


class TransitionTable:
    """Handle transitions around one policy time_step for population projection modeling"""

    def __init__(
        self,
        policy_time_step: int,
        policy_list: List[
            Any
        ],  # this should actually be List[SparkPolicy] but I can't say that without circular dependence
        previous_tables: Optional[Dict[int, pd.DataFrame]] = None,
    ):
        """
        policy_time_step: time-step at which the related policy is enacted
        policy_list: list of SparkPolicy that apply on policy_time_step
        max_sentence: length of transition tables
        previous_tables: preceding TransitionTable. Should only be None (or empty dict) for chronologically first table
        """
        if previous_tables is None:
            previous_tables = {}
        elif not isinstance(previous_tables, Dict):
            raise TypeError(
                f"previous_tables is not a Dict[int, pd.DataFrame]: {previous_tables!r}"
            )
        self.previous_tables: Dict[int, pd.DataFrame] = previous_tables
        self.policy_time_step = policy_time_step
        self.policy_list = policy_list

        self.max_sentence = max(
            (int(np.ceil(t.index.max())) for t in self.previous_tables.values()),
            default=0,
        )

        self.tables: Dict[int, pd.DataFrame] = collections.OrderedDict()

        self._initialize_tables()

    def _initialize_tables(self) -> None:
        """Generate `before`, `transitory`, and `after` transition tables."""

        # if chronologically first table, no policies and only 'after' needs to be populated, which is handled elsewhere
        if not self.previous_tables:
            return

        for previous_table in self.previous_tables.values():
            if "remaining" in previous_table:
                raise ValueError(
                    "Cannot create a transition table from a normalized previous table"
                )

        # Apply all the non-retroactive policy functions to the 'before' table
        for time_step, prev_table in sorted(self.previous_tables.items()):
            self.tables[time_step] = prev_table.copy()
            if time_step >= self.policy_time_step:
                raise ValueError(
                    f"Time step {time_step} in previous_tables ({self.previous_tables.keys()}) \
                    is greater than policy time step {self.policy_time_step}!"
                )

        for policy in self.policy_list:
            if policy.apply_retroactive:
                policy.policy_fn(self)

        # Store a copy of the transition tables before non-retroactive policies are applied in order to validate
        # afterward that the non-retroactive policy did not modify the transition data before the policy ts
        transition_table_no_policy_copy = {
            ts: t.copy() for ts, t in self.tables.items()
        }

        self.tables[self.policy_time_step] = self.tables[max(self.tables)].copy()
        # Add the non-retroactive policy functions to the 'after' table
        for policy in self.policy_list:
            if not policy.apply_retroactive:
                policy.policy_fn(self)

        # Assert that the non-retroactive policies did not change pre-policy time_step transition tables
        if any(
            not transition_table_no_policy_copy[ts].equals(
                transition_df.iloc[: len(transition_table_no_policy_copy[ts])]
            )
            for ts, transition_df in self.tables.items()
            if ts != self.policy_time_step
        ):
            raise ValueError(
                "Policy function was applied to the wrong transition time_step."
            )

        self.transitory_table = self._collapse_tables(
            self.tables, self.policy_time_step
        )

    def normalize_table(
        self, time_step: int, before_table: Optional[pd.DataFrame] = None
    ) -> None:
        self.tables[time_step] = self.normalized_table(
            self.tables[time_step], self.max_sentence, before_table
        )

    def normalize_previous_tables(
        self, before_table: Optional[pd.DataFrame] = None
    ) -> None:
        for time_step, prev_table in self.previous_tables.items():
            self.previous_tables[time_step] = self.normalized_table(
                prev_table, self.max_sentence, before_table
            )

    @staticmethod
    def normalized_table(
        table: pd.DataFrame,
        max_sentence: int,
        before_table: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Convert the per-time_step population counts into normalized probabilities"""
        # before_table_input = None if before_table is None else before_table.copy()
        if table.empty:
            raise ValueError(
                "Cannot normalize transition tables before they are initialized"
            )

        if "remaining" in table:
            raise ValueError(
                f"Trying to normalize a transition table that is already normalized: {table}"
            )

        if before_table is None:
            before_table = table.copy()

        if len(before_table) < len(table):
            before_table = before_table.reindex(table.index, fill_value=0)

        if "remaining" in before_table:
            raise ValueError(
                "`normalize_transitions` cannot be called with a normalized `before_table`"
            )

        before_totals = before_table.sum()
        after_totals = table.sum()

        outflow_ratios = after_totals / after_totals.sum()

        # want to release number of people X that causes (before_counteds at time t) + X to equal
        # (after_counteds at time t+1), so we start by stepping forward after_counteds to t+1
        before_counteds = before_table.shift(1, fill_value=0.0).cumsum()
        after_counteds = table.cumsum()
        # check that last acounted value = after_totals
        # calculate the fraction of each outflow remaining for each time step
        after_remaining = 1 - after_counteds / after_totals
        after_remaining.clip(0, 1, inplace=True)
        before_remaining = 1 - before_counteds / before_totals
        before_remaining.clip(0, 1, inplace=True)

        # Calculate X from above for each outflow, then scale by size of 'cohort' for that outflow
        outflow_scaled_releases = (before_remaining - after_remaining) * outflow_ratios

        # In order to normalize, we also need to calculate the total remaining, also scaled by outflows
        outflow_scaled_remaining = (before_remaining * outflow_ratios).sum(axis=1)

        # combine the above to get the release probabilities for each time step
        # Note: we allow division by zero above when before_remaining is 0, but in that case no one is left to be
        # influenced by this table of probabilities, so we can trivially fillna with 0s.
        normalized_df = outflow_scaled_releases.div(outflow_scaled_remaining, axis=0)
        normalized_df.fillna(0.0, inplace=True)
        normalized_df.clip(0, 1, inplace=True)
        normalized_df = normalized_df.apply(lambda x: round(x, SIG_FIGS))

        # Assign the residual probability as the proportion that remains in the current compartment per month
        normalized_df["remaining"] = 1 - round(normalized_df.sum(axis=1), SIG_FIGS - 1)

        # Check that the transition table is valid
        full_release_times = np.isclose(normalized_df["remaining"], 0, SIG_FIGS)

        if full_release_times.sum() == 0:
            # KNOWN ISSUE: presumed rounding error for valid transitions table
            # use this commented hack below if happening

            # before_table_input.to_csv('~/Desktop/debug/before_table_input.csv')
            # normalized_df.to_csv('~/Desktop/debug/normalized_df.csv')
            # outflow_scaled_releases.to_csv('~/Desktop/debug/outflow_scaled_releases.csv')
            # table.to_csv('~/Desktop/debug/table.csv')
            # outflow_scaled_remaining.to_csv('~/Desktop/debug/outflow_scaled_remaining.csv')
            # after_remaining.to_csv('~/Desktop/debug/after_remaining.csv')
            # before_remaining.to_csv('~/Desktop/debug/before_remaining.csv')
            # before_table.to_csv('~/Desktop/debug/before_table.csv')

            # normalized_df = outflow_scaled_releases.div(
            #     outflow_scaled_remaining, axis=0
            # )

            # normalized_df.liberty = normalized_df.liberty.replace(
            #     [np.inf, -np.inf], np.NaN
            # ).fillna(1)

            # normalized_df.fillna(0.0, inplace=True)
            # normalized_df.clip(0, 1, inplace=True)
            # normalized_df = normalized_df.apply(lambda x: round(x, SIG_FIGS))

            # # Assign the residual probability as the proportion that remains in the current compartment per month
            # normalized_df["remaining"] = 1 - round(
            #     normalized_df.sum(axis=1), SIG_FIGS - 1
            # )

            # # Check that the transition table is valid
            # full_release_times = np.isclose(normalized_df["remaining"], 0, SIG_FIGS)

            # if full_release_times.sum() == 0:
            #     import pdb

            #     pdb.set_trace()
            #     raise ValueError(
            #         f"Transition table doesn't release everyone: "
            #         f"{normalized_df.iloc[-1]}"
            #     )

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
                    f"'{compartment}' transition has probabilities out of bounds:\n"
                    f"{erroneous_values}"
                )
        min_of_index = min(normalized_df[full_release_times].index)
        max_sentence = min(max_sentence, min_of_index)
        normalized_df.loc[max_sentence, "remaining"] = 0

        return normalized_df

    def normalize_transitions(self) -> None:
        """standardize the transition probabilities for all transition tables"""
        if self.previous_tables:
            before_table = self._collapse_tables(
                self.previous_tables, self.policy_time_step
            )
            self.transitory_table = TransitionTable.normalized_table(
                self.transitory_table, self.max_sentence, before_table=before_table
            )
        for time_step, table in self.tables.items():
            self.tables[time_step] = TransitionTable.normalized_table(
                table, self.max_sentence
            )

    def generate_transition_tables(
        self, time_steps: List[int], historical_outflows: pd.DataFrame
    ) -> None:
        """
        Accepts time_steps for which to populate transition probabilities
        Accepts a DataFrame with transition data to use
        Should only be called for chronologically first TransitionTable in a baseline simulation
        """
        sentence_length = historical_outflows["compartment_duration"].apply(np.ceil)

        grouped_outflows = historical_outflows.groupby(
            [sentence_length, historical_outflows.outflow_to]
        )

        self.max_sentence = max(self.max_sentence, int(sentence_length.max()))

        for time_step in time_steps:
            self.tables[time_step] = (
                grouped_outflows["cohort_portion"]
                .sum()
                .unstack()
                .reindex(range(1, self.max_sentence + 1))
                .fillna(0)
            )

    def get_after_table(self) -> pd.DataFrame:
        return self.tables[self.policy_time_step]

    def get_per_time_step_table(self, current_time_step: int) -> pd.DataFrame:
        """Returns a combination of transition_dfs for the given time_step"""
        for time_step, table in self.tables.items():
            if "remaining" not in table.columns:
                raise ValueError(
                    f"Transition table for time_step '{time_step}' has not been normalized"
                )

        if current_time_step < self.policy_time_step:
            raise ValueError(
                f"Trying to use transition table for policy_time_step {self.policy_time_step} too early \
                (current_time_step: {current_time_step})"
            )

        # first time_step of policy, need to transition from one table to the other
        if current_time_step == self.policy_time_step:
            return self.transitory_table

        return self._collapse_tables(self.tables, current_time_step)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, TransitionTable):
            return False
        for time_step, table in self.tables.items():
            if set(table.columns) != set(other.tables[time_step].columns):
                return False
            matching_order_self = table[other.tables[time_step].columns]
            if (matching_order_self != other.tables[time_step]).all().all():
                return False
        return True

    def _check_table_invariant_before_normalization(self, time_step: int) -> None:
        """Raise an exception if the underlying transition table has been normalized before applying a policy"""
        self._check_table_exists(time_step)
        table = self.tables[time_step]
        if (table is None) or "remaining" in table:
            raise ValueError(
                "Policy method cannot be applied after the transition table is normalized"
            )

    def _check_table_exists(self, time_step: int) -> None:
        """Make sure table is not null"""
        table = self.tables.get(time_step)
        if table is None:
            raise ValueError(
                f"Cannot normalize time_step {time_step} table for first TransitionTable of \
                CompartmentTransitions object"
            )

    @staticmethod
    def _collapse_tables(
        tables: Dict[int, pd.DataFrame], current_time_step: int
    ) -> pd.DataFrame:
        """
        Concatenate the pieces of historical tables that apply to the different cohorts
            in a given time_step `current_time_step`
        """
        partial_tables = []
        prev_time_step_since_policy_plus_1 = None
        for time_step, table in sorted(tables.items(), reverse=True):
            time_step_since_policy = current_time_step - time_step
            partial_tables.append(
                table.loc[prev_time_step_since_policy_plus_1:time_step_since_policy]
            )
            prev_time_step_since_policy_plus_1 = time_step_since_policy + 1
        return pd.concat(partial_tables)

    def get_time_steps_from_retroactive(self, retroactive: bool) -> List[int]:
        if retroactive:
            return [ts for ts in self.tables if ts != self.policy_time_step]
        return [self.policy_time_step]

    def test_non_retroactive_policy(self) -> None:
        for time_step in self.get_time_steps_from_retroactive(False):
            self.tables[time_step]["jail"] = 0

    def test_retroactive_policy(self) -> None:
        for time_step in self.get_time_steps_from_retroactive(True):
            self.tables[time_step]["jail"] = 0

    def extend_tables(self, new_max_sentence: int) -> None:
        """
        extends the max sentence of transition tables to `new_max_sentence` --> doesn't handle long-sentences
            correctly but those should get taken out anyway
        doesn't shorten max sentence if new_max_sentence is smaller than self.max_sentence
        """
        # Ensure none of the transition tables have been normalized before they are extended
        for time_step, table in self.tables.items():
            if not table.empty:
                self._check_table_invariant_before_normalization(time_step)

        if new_max_sentence <= self.max_sentence:
            return

        for time_step, table in self.tables.items():
            # Extend the populated transition tables
            if not table.empty:
                extended_index = pd.Index(
                    data=range(self.max_sentence + 1, new_max_sentence + 1),
                    name=table.index.name,
                )
                extended_df = pd.DataFrame(
                    index=extended_index,
                    columns=table.columns,
                )
                self.tables[time_step] = pd.concat([table, extended_df]).fillna(0)

        self.max_sentence = new_max_sentence

    @staticmethod
    def unnormalized_table(table: pd.DataFrame) -> pd.DataFrame:
        """Revert a normalized table back to an un-normalized df. The sum of all total populations will be 1"""
        if "remaining" not in table:
            raise ValueError("Trying to un-normalize a table that isn't normalized")

        for sentence_length in table.index.sort_values(ascending=False):
            table.loc[sentence_length] *= np.prod(
                1
                - table.loc[range(1, sentence_length)]
                .drop("remaining", axis=1)
                .sum(axis=1)
            )

        return table.drop("remaining", axis=1)

    def unnormalize_previous_tables(self) -> None:
        """revert all normalized previous table back to an un-normalized df. sum of all total populations will be 1"""
        if not self.previous_tables:
            raise ValueError("There are no previous_tables to unnormalize!")
        for time_step, table in self.previous_tables.items():
            self.previous_tables[time_step] = TransitionTable.unnormalized_table(table)

    def unnormalize_table(self, time_step: int) -> None:
        """revert a normalized table back to an un-normalized df. sum of all total populations will be 1"""
        self._check_table_exists(time_step)
        self.tables[time_step] = TransitionTable.unnormalized_table(
            self.tables[time_step]
        )

    def use_alternate_transitions_data(
        self, alternate_historical_transitions: pd.DataFrame, retroactive: bool
    ) -> None:
        """Replace the historical admission data for this specific group with another data from a different set"""
        self.extend_tables(
            int(alternate_historical_transitions.compartment_duration.max())
        )
        time_steps = self.get_time_steps_from_retroactive(retroactive)
        self.generate_transition_tables(time_steps, alternate_historical_transitions)

    def preserve_normalized_outflow_behavior(
        self,
        outflows: List[str],
        time_step: int,
        before_time_step: Optional[int] = None,
    ) -> None:
        """
        change the transition probabilities for outflows so the yearly (normalized) percentage of those outflows per
            year match 'before' state
        `outflows` should be a list of outflows to affect
        `time_step` transition table time step for which to affect them
        `before_time_step` transition table time step to copy normalized behavior from
            xor None to use the most recent table in self.previous_tables
        """
        self._check_table_exists(time_step)
        self._check_table_invariant_before_normalization(time_step)

        if time_step == before_time_step:
            raise ValueError(
                f"matching time_step to itself ({time_step=}, {before_time_step=})"
            )

        if before_time_step is None:
            self.normalize_previous_tables()
            # Use the latest previous_table before policy_time_step for normalization
            before_table = self.previous_tables[
                max(ts for ts in self.previous_tables if ts != self.policy_time_step)
            ]
        else:
            self.normalize_table(before_time_step)
            before_table = self.tables[before_time_step]
        self.normalize_table(time_step)

        if self.max_sentence <= 0:
            raise ValueError(f"Max sentence length is not set: {self.max_sentence=}")

        old_total_outflows = (
            self.tables[time_step].drop("remaining", axis=1).sum(axis=1)
        )
        old_total_remaining = (1 - old_total_outflows).cumprod()

        # set normalized probabilities equal to 'before' for desired outflows
        self.tables[time_step].loc[:, outflows] = before_table.loc[:, outflows]

        new_total_outflows = (
            self.tables[time_step].drop("remaining", axis=1).sum(axis=1)
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
            self.tables[time_step].loc[
                sentence_length, ~self.tables[time_step].columns.isin(outflows)
            ] *= (
                old_total_remaining[sentence_length - 1]
                / new_total_remaining[sentence_length - 1]
            )

        # revert altered table back to un-normalized
        if before_time_step is None:
            self.unnormalize_previous_tables()
        else:
            self.unnormalize_table(before_time_step)
        self.unnormalize_table(time_step)

    def apply_reductions(
        self,
        reduction_df: pd.DataFrame,
        reduction_type: str,
        affected_LOS: Optional[List[Optional[int]]] = None,
        retroactive: bool = False,
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

        if affected_LOS is None:
            affected_LOS = [None, None]

        if len(affected_LOS) != 2:
            raise ValueError("affected_LOS must have exactly two elements.")

        if (affected_LOS[0] is None) or (affected_LOS[0] < 1):
            affected_LOS[0] = 1

        if (affected_LOS[1] is None) or (affected_LOS[1] > self.max_sentence + 1):
            affected_LOS[1] = self.max_sentence + 1

        time_steps = self.get_time_steps_from_retroactive(retroactive)

        for time_step in time_steps:
            self._check_table_invariant_before_normalization(time_step)

        for _, row in reduction_df.iterrows():
            for sentence_length in range(affected_LOS[0], affected_LOS[1]):  # type: ignore
                for time_step in time_steps:
                    # record population to re-distribute
                    sentence_count = (
                        self.tables[time_step].loc[sentence_length, row.outflow]
                        * row.affected_fraction
                    )

                    # start by clearing df entry that's getting re-distributed
                    self.tables[time_step].loc[sentence_length, row.outflow] *= (
                        1 - row.affected_fraction
                    )

                    # calculate new sentence length
                    if reduction_type == "*":
                        new_sentence_length = max(
                            [sentence_length * (1 - row.reduction_size), 1]
                        )
                    elif reduction_type == "+":
                        new_sentence_length = max(
                            [sentence_length - row.reduction_size, 1]
                        )
                    else:
                        raise RuntimeError(
                            f"reduction type {reduction_type} not recognized (must be '*' or '+')"
                        )

                    # separate out non-integer sentence length into one chunk rounded up and one chunk rounded down,
                    #   weighted by where in the middle actual sentence falls
                    longer_bit = (new_sentence_length % 1) * sentence_count
                    shorter_bit = sentence_count - longer_bit

                    # add in new sentence length probabilities to the df
                    self.tables[time_step].loc[
                        int(new_sentence_length), row.outflow
                    ] += shorter_bit
                    if longer_bit > 0:
                        self.tables[time_step].loc[
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
        time_steps = self.get_time_steps_from_retroactive(retroactive)
        # ensure tables haven't been normalized already
        for time_step in time_steps:
            self._check_table_invariant_before_normalization(time_step)

            if (
                reallocation_type == "*"
                and not reallocation_df.new_outflow.isin(self.tables[time_step]).all()
            ):
                raise ValueError(
                    "Cannot use scaling methodology if new_outflow not already in transition table"
                )

            for _, row in reallocation_df.iterrows():
                before_outflow = np.array(self.tables[time_step][row.outflow])
                self.tables[time_step][row.outflow] = list(
                    before_outflow * (1 - row.affected_fraction)
                )

                if not row.isnull().new_outflow:
                    if reallocation_type == "+":
                        new_outflow_value = list(
                            np.array(self.tables[time_step].get(row.new_outflow, 0))
                            + before_outflow * row.affected_fraction
                        )
                        self.tables[time_step][row.new_outflow] = new_outflow_value
                    elif reallocation_type == "*":
                        reallocated_population = (
                            sum(before_outflow) * row.affected_fraction
                        )
                        new_outflow_population = sum(
                            self.tables[time_step][row.new_outflow]
                        )
                        scale_factor = (
                            1 + reallocated_population / new_outflow_population
                        )
                        updated_new_outflow = (
                            np.array(self.tables[time_step][row.new_outflow])
                            * scale_factor
                        )
                        self.tables[time_step][row.new_outflow] = list(
                            updated_new_outflow
                        )

                    else:
                        raise RuntimeError(
                            f"reallocation type {reallocation_type} not recognized (must be '*' or '+')"
                        )

    def abolish_mandatory_minimum(
        self,
        historical_outflows: pd.DataFrame,
        outflow: str,
        current_mm: Optional[float] = None,
        affected_fraction: Optional[float] = None,
        retroactive: bool = False,
    ) -> None:
        """
        Reduce compartment durations as associated with a mandatory minimum reduction/removal. Our methodology for
            this involves scaling down the entire distribution additively by the product of distribution std dev and
            fraction of population sentenced at the mandatory minimum.
        affected_fraction is an optional input to restrict the shift (total magnitude preserved) to a subset of the
            distribution. Note that you must pass exactly one of current_mm, affected_fraction
        To tell the method to assume the mode compartment_sentence is the current mm, set `current_mm`='auto'
        """
        if current_mm is not None:
            if current_mm == "auto":
                current_mm = (
                    historical_outflows.sort_values("cohort_portion")
                    .iloc[-1]
                    .compartment_duration
                )

            mm_sentenced_group = historical_outflows[
                historical_outflows.compartment_duration == current_mm
            ]

            # Do not modify the transition table if there are no sentences at the mandatory minimum
            if len(mm_sentenced_group) == 0:
                return

            affected_ratio = (
                mm_sentenced_group["cohort_portion"].sum()
                / historical_outflows["cohort_portion"].sum()
            )

            if affected_fraction is not None:
                raise ValueError("Cannot set both current_mm and affected_fraction")

        elif affected_fraction is not None:
            affected_ratio = affected_fraction
        else:
            raise ValueError(
                "Expected either current_mm or affected_ratio to be nonnull."
            )

        # calculate standard deviation
        average_duration = np.average(
            historical_outflows.compartment_duration,
            weights=historical_outflows.cohort_portion,
        )
        variance = np.average(
            (historical_outflows.compartment_duration - average_duration) ** 2,
            weights=historical_outflows.cohort_portion,
        )
        std = np.sqrt(variance)

        mm_factor = affected_ratio * std / 2

        self.apply_reductions(
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
        time_steps = self.get_time_steps_from_retroactive(retroactive)
        for time_step in time_steps:
            technical_transitions = self.tables[time_step][technical_outflow]
            release_transitions = self.tables[time_step][release_outflow]

            # get max completion duration
            max_release_duration = release_transitions[
                release_transitions > 0
            ].index.max()
            chopped_indices = technical_transitions.index > max_release_duration

            # get population to reallocate
            chopped_population = technical_transitions[chopped_indices].sum()

            # chop technicals
            self.tables[time_step].loc[chopped_indices, technical_outflow] = 0

            # reallocate to release
            self.tables[time_step].loc[
                max_release_duration, release_outflow
            ] += chopped_population
