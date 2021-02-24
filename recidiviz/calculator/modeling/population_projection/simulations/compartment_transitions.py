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


from copy import deepcopy
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy

SIG_FIGS = 7


class CompartmentTransitions:
    """Handle transition tables for one compartment that sends groups to multiple other compartments over time"""

    def __init__(self, historical_outflows: pd.DataFrame) -> None:
        required_columns = ['outflow_to', 'compartment', 'compartment_duration', 'total_population']
        missing_columns = [col for col in required_columns if col not in historical_outflows.columns]
        if len(missing_columns) != 0:
            raise ValueError(f"historical_outflows dataframe is missing the required columns {required_columns}")

        if 'remaining' in historical_outflows['outflow_to'].unique():
            raise ValueError("historical_outflows dataframe cannot contain an outflow column named `remaining`")

        self.max_sentence: int = 0

        self.outflows = historical_outflows['outflow_to'].unique()

        self.historical_outflows = historical_outflows

        self.transition_dfs: Dict[str, pd.DataFrame] = {}

        self._initialize_transition_table()

    def _initialize_transition_table(self) -> None:
        """Populate the 'before' transition table and initializes the max_sentence from historical data """
        if self.historical_outflows.empty:
            raise ValueError("Cannot create a transition table with an empty transitions_data dataframe")
        for column in ['total_population', 'compartment_duration']:
            if any(self.historical_outflows[column] < 0):
                negative_rows = self.historical_outflows[self.historical_outflows[column] < 0]
                raise ValueError(f"Transition data '{column}' column cannot contain negative values {negative_rows}")
            if any(self.historical_outflows[column].isnull()):
                null_rows = self.historical_outflows[self.historical_outflows[column].isnull()]
                raise ValueError(f"Transition data '{column}' column cannot contain NULL values {null_rows}")

        self.max_sentence = int(np.ceil(self.historical_outflows.compartment_duration.max()))

        self.transition_dfs = {
            'before': pd.DataFrame({outflow: np.zeros(self.max_sentence) for outflow in self.outflows},
                                   index=range(1, self.max_sentence + 1)),
            'transitory': pd.DataFrame(),
            'after_retroactive': pd.DataFrame(),
            'after_non_retroactive': pd.DataFrame()
        }

        self._generate_transition_table(state='before')

    def _generate_transition_table(self, state: str, historical_outflows: Optional[pd.DataFrame] = None) -> None:
        """
        Accepts a state for which to populate transition probabilities
        optionally accepts a DataFrame with alternative transition data to use
        """

        if historical_outflows is None:
            historical_outflows = self.historical_outflows

        sentence_length = historical_outflows['compartment_duration'].apply(np.ceil)

        grouped_outflows = historical_outflows.groupby([sentence_length, historical_outflows.outflow_to])

        self.transition_dfs[state] = grouped_outflows['total_population'].sum().unstack().reindex(
            range(1, self.max_sentence + 1)).fillna(0)

    def normalize_transitions(self, state: str, before_table: Optional[pd.DataFrame] = None) -> None:
        """Convert the per-ts population counts into normalized probabilities"""
        if (state not in self.transition_dfs) or self.transition_dfs[state].empty:
            raise ValueError("Cannot normalize transition tables before they are initialized")

        if 'remaining' in self.transition_dfs[state]:
            raise ValueError(
                f"Trying to normalize a transition table that is already normalized: {self.transition_dfs[state]}")

        if before_table is None:
            before_table = self.transition_dfs[state]

        if 'remaining' in before_table:
            raise ValueError("`normalize_transitions` cannot be called with a normalized `before_table`")

        normalized_df = deepcopy(self.transition_dfs[state])
        after_table = self.transition_dfs[state]

        before_counted = 0
        after_counted = 0

        before_total = before_table.sum().sum()
        after_total = after_table.sum().sum()

        outflow_weights = before_table.sum() / before_total

        for sentence_len in range(1, self.max_sentence + 1):

            if (before_counted == before_total) or (after_counted == after_total):
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

        self.transition_dfs[state] = normalized_df

        self.transition_dfs[state] = self.transition_dfs[state].apply(lambda x: round(x, SIG_FIGS))

        # Assign the residual probability as the proportion that remains in the current compartment per month
        self.transition_dfs[state]['remaining'] = 1 - round(self.transition_dfs[state].sum(axis=1), SIG_FIGS - 1)
        if self.transition_dfs[state].loc[self.max_sentence, 'remaining'] > 10 ** (-SIG_FIGS):
            raise ValueError(f"Final row of transition table doesn't release everyone: "
                             f"{self.transition_dfs[state].iloc[-1]}")

        self.transition_dfs[state].loc[self.max_sentence, 'remaining'] = 0

        # Make sure all transition probabilities are between 0-1
        for compartment, transition_df in self.transition_dfs[state].items():
            if any((transition_df < 0) | (transition_df > 1)):
                erroneous_values = transition_df[(transition_df < 0) | (transition_df > 1)]
                raise ValueError(f"'{compartment}' transition has probabilities out of bounds for state '{state}':\n"
                                 f"{erroneous_values}")

    def initialize(self, compartment_policies: List[SparkPolicy]) -> None:
        """Initialize all transition tables given a list of SparkPolicy."""
        self.transition_dfs['after_retroactive'] = deepcopy(self.transition_dfs['before'])

        # apply all the relevant retroactive policy functions to the transition class
        for policy in compartment_policies:
            if policy.apply_retroactive:
                policy.policy_fn(self)

        # apply the rest of the relevant policy functions to the transition class
        self.transition_dfs['after_non_retroactive'] = deepcopy(self.transition_dfs['after_retroactive'])
        before_after_retroactive = deepcopy(self.transition_dfs['after_retroactive'])
        for policy in compartment_policies:
            if not policy.apply_retroactive:
                policy.policy_fn(self)

        # make sure policy affects correct transition table
        if any(list(before_after_retroactive[i]) !=
                list(self.transition_dfs['after_retroactive'][i])[:len(before_after_retroactive[i])]
                for i in self.transition_dfs['after_retroactive']):
            raise ValueError("Policy function was applied to the wrong transition state")

        # generate the transitory table for retroactive policies
        self.transition_dfs['transitory'] = deepcopy(self.transition_dfs['after_retroactive'])

        # standardize the transition probabilities for all transition classes
        self.normalize_transitions('transitory', before_table=self.transition_dfs['before'])
        self.normalize_transitions('before')
        self.normalize_transitions('after_retroactive')
        self.normalize_transitions('after_non_retroactive')

    def get_per_ts_transition_table(self, current_ts: int, policy_ts: int) -> pd.DataFrame:
        """function used by SparkCompartment to determine which of the state transition tables to pull from"""
        for state in self.transition_dfs:
            if 'remaining' not in self.transition_dfs[state].columns:
                raise ValueError(f"Transition table for the '{state}' has not been normalized")

        # pre policy implementation
        if current_ts < policy_ts:
            return self.transition_dfs['before']

        # first ts of policy, need to transition from one table to the other
        if current_ts == policy_ts:
            return self.transition_dfs['transitory']

        # post policy implementation
        ts_since_policy = current_ts - policy_ts
        return pd.concat([self.transition_dfs['after_non_retroactive'].loc[:ts_since_policy, :],
                          self.transition_dfs['after_retroactive'].loc[ts_since_policy + 1:, :]])

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CompartmentTransitions):
            return False
        for state in self.transition_dfs:
            if set(self.transition_dfs[state].columns) != set(other.transition_dfs[state].columns):
                return False
            matching_order_self = self.transition_dfs[state][other.transition_dfs[state].columns]
            if (matching_order_self != other.transition_dfs[state]).all().all():
                return False
        return True

    @staticmethod
    def get_df_name_from_retroactive(retroactive: bool) -> str:
        if retroactive:
            return 'after_retroactive'

        return 'after_non_retroactive'

    def test_non_retroactive_policy(self) -> None:
        self.transition_dfs['after_non_retroactive']['jail'] = 0

    def test_retroactive_policy(self) -> None:
        self.transition_dfs['after_retroactive']['jail'] = 0

    def extend_tables(self, new_max_sentence: int) -> None:
        """
        extends the max sentence of transition tables to `new_max_sentence` --> doesn't handle long-sentences
            correctly but those should get taken out anyway
        doesn't shorten max sentence if new_max_sentence is smaller than self.max_sentence
        """
        # Ensure none of the transition tables have been normalized before they are extended
        for state in self.transition_dfs:
            if not self.transition_dfs[state].empty:
                self.check_table_invariant_before_normalization(state)

        if new_max_sentence <= self.max_sentence:
            return

        for state in self.transition_dfs:
            # skip over un-populated tables
            if not self.transition_dfs[state].empty:

                self.transition_dfs[state] = self.transition_dfs[state].append(
                    pd.DataFrame(index=range(self.max_sentence + 1, new_max_sentence + 1))).fillna(0)

        self.max_sentence = new_max_sentence

    def unnormalize_table(self, state: str) -> None:
        """revert a normalized table back to an un-normalized df. sum of all total populations will be 1"""
        if 'remaining' not in self.transition_dfs[state]:
            raise ValueError("trying to unnormalize a table that isn't normalized")

        for sentence_length in self.transition_dfs[state].index.sort_values(ascending=False):
            for shorter_sentence in range(1, sentence_length):
                self.transition_dfs[state].loc[sentence_length] *= \
                    1 - self.transition_dfs[state].loc[shorter_sentence].drop('remaining').sum()

        self.transition_dfs[state] = self.transition_dfs[state].drop('remaining', axis=1)

    def use_alternate_transitions_data(self, alternate_historical_transitions: pd.DataFrame, retroactive: bool) -> None:
        """Replace the historical admission data for this specific group with another data from a different set"""

        self.extend_tables(int(alternate_historical_transitions.compartment_duration.max()))
        df_name = self.get_df_name_from_retroactive(retroactive)
        self._generate_transition_table(df_name, alternate_historical_transitions)

    def preserve_normalized_outflow_behavior(self, outflows: List[str], state: str,
                                             before_state: str = 'before') -> None:
        """
        change the transition probabilities for outflows so the yearly (normalized) percentage of those outflows per
            year match 'before' state
        `outflows` should be a list of outflows to affect
        `state` should be the transition table for which to affect them
        `before_state` should be the transition table to copy normalized behavior from
         """
        for column in [state, 'before']:
            if 'remaining' in self.transition_dfs[column]:
                raise ValueError(f"{column} transition table has already been normalized")

        if state == before_state:
            raise ValueError("matching state to itself")

        if self.max_sentence <= 0:
            raise ValueError("Max sentence length is not set")

        self.normalize_transitions(before_state)
        self.normalize_transitions(state)

        old_total_outflows = self.transition_dfs[state].drop('remaining', axis=1).sum(axis=1)
        old_total_remaining = (1 - old_total_outflows).cumprod()

        # set normalized probabilities equal to 'before' for desired outflows
        self.transition_dfs[state].loc[:, outflows] = self.transition_dfs[before_state].loc[:, outflows]

        new_total_outflows = self.transition_dfs[state].drop('remaining', axis=1).sum(axis=1)
        # will change as we go, so can't cumprod all at once
        new_total_remaining = 1 - new_total_outflows

        # re-normalize other outflows
        for sentence_length in range(2, self.max_sentence + 1):
            # if no remaining, we're done
            #   only need to check one of the two because scaling will never bring outflows from non-zero to zero
            if new_total_remaining[sentence_length] == 0:
                break

            # cumprod happens here, step by step as the df is updated
            new_total_remaining[sentence_length] *= new_total_remaining[sentence_length - 1]

            # re-normalize un-affected outflows for this row
            self.transition_dfs[state].loc[sentence_length, ~self.transition_dfs[state].columns.isin(outflows)] *= \
                old_total_remaining[sentence_length - 1] / new_total_remaining[sentence_length - 1]

        # revert altered table back to un-normalized
        self.unnormalize_table(before_state)
        self.unnormalize_table(state)

    def check_table_invariant_before_normalization(self, state: str) -> None:
        """Raise an exception if the underlying transition table has been normalized before applying a policy"""
        if 'remaining' in self.transition_dfs[state]:
            raise ValueError("Policy method cannot be applied after the transition table is normalized")

    def check_table_invariant_after_normalization(self, state: str) -> None:
        """Make sure transition lists are all positive decimal probability values for the provided transition state"""
        if (self.transition_dfs[state] < 0).any().any() or (self.transition_dfs[state] > 1).any().any():
            raise ValueError(f"All `{state}` probabilities must be between 0 and 1\n"
                             f"{self.transition_dfs[state]}")

    def apply_reduction(self, reduction_df: pd.DataFrame, reduction_type: str,
                        retroactive: bool = False) -> None:
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
        if 'remaining' in self.transition_dfs['before']:
            raise ValueError("Reduction policy cannot be applied to a normalized transition table")

        if self.max_sentence <= 0:
            raise ValueError("Max sentence length is not set")

        df_name = self.get_df_name_from_retroactive(retroactive)

        for _, row in reduction_df.iterrows():
            for sentence_length in range(1, self.max_sentence + 1):
                # record population to re-distribute
                sentence_count = self.transition_dfs[df_name].loc[sentence_length, row.outflow] * row.affected_fraction

                # start by clearing df entry that's getting re-distributed
                self.transition_dfs[df_name].loc[sentence_length, row.outflow] *= 1 - row.affected_fraction

                # calculate new sentence length
                if reduction_type == '*':
                    new_sentence_length = max([sentence_length * (1 - row.reduction_size), 1])
                elif reduction_type == '+':
                    new_sentence_length = max([sentence_length - row.reduction_size, 1])
                else:
                    raise RuntimeError(f"reduction type {reduction_type} not recognized (must be '*' or '+')")

                # separate out non-integer sentence length into one chunk rounded up and one chunk rounded down,
                #   weighted by where in the middle actual sentence falls
                longer_bit = (new_sentence_length % 1) * sentence_count
                shorter_bit = sentence_count - longer_bit

                # add in new sentence length probabilities to the df
                self.transition_dfs[df_name].loc[int(new_sentence_length), row.outflow] += shorter_bit
                if longer_bit > 0:
                    self.transition_dfs[df_name].loc[int(new_sentence_length) + 1, row.outflow] += longer_bit

    def reallocate_outflow(self, reallocation_df: pd.DataFrame, reallocation_type: str,
                           retroactive: bool = False) -> None:
        """
        reallocation_df should be a df with columns
            'outflow': outflow to be reallocated
            'affected_fraction' : 0 =< float =< 1
            'new_outflow': outflow_tag
        reallocation_type: '*' or '+' --> if '*', scale new_outflow. if '+', add original sentence distribution
        If `new_outflow` doesn't exist, create a new column for it. If null, just scale down (BE VERY USING NULL,
            EASY TO MESS UP NORMALIZATION)
        """
        # ensure tables haven't been normalized already
        if 'remaining' in self.transition_dfs['before']:
            raise ValueError("Outflow cannot be reallocated on a normalized transition table")

        df_name = self.get_df_name_from_retroactive(retroactive)

        if reallocation_type == '*' \
                and not reallocation_df.new_outflow.isin(self.transition_dfs[df_name]).all():
            raise ValueError("Cannot use scaling methodology if new_outflow not already in transition table")

        for _, row in reallocation_df.iterrows():
            before_outflow = np.array(self.transition_dfs[df_name][row.outflow])
            self.transition_dfs[df_name][row.outflow] = list(before_outflow * (1 - row.affected_fraction))

            if not row.isnull().new_outflow:
                if reallocation_type == '+':
                    new_outflow_value = list(np.array(self.transition_dfs[df_name].get(row.new_outflow, 0)) +
                                             before_outflow * row.affected_fraction)
                    self.transition_dfs[df_name][row.new_outflow] = new_outflow_value
                elif reallocation_type == '*':
                    reallocated_population = sum(before_outflow) * row.affected_fraction
                    new_outflow_population = sum(self.transition_dfs[df_name][row.new_outflow])
                    scale_factor = 1 + reallocated_population / new_outflow_population
                    updated_new_outflow = np.array(self.transition_dfs[df_name][row.new_outflow]) * scale_factor
                    self.transition_dfs[df_name][row.new_outflow] = list(updated_new_outflow)

                else:
                    raise RuntimeError(f"reallocation type {reallocation_type} not recognized (must be '*' or '+')")

    def abolish_mandatory_minimum(self, current_mm: float, outflow: str, retroactive: bool = False) -> None:
        """
        Reduce compartment durations as associated with a mandatory minimum reduction/removal. Our methodology for
            this involves scaling down the entire distribution additively by the product of distribution std dev and
            fraction of population sentenced at the mandatory minimum.
        """
        mm_sentenced_group = self.historical_outflows[self.historical_outflows.compartment_duration == current_mm]
        # Do not modify the transition table if there are no sentences at the mandatory minimum
        if len(mm_sentenced_group) == 0:
            return

        affected_ratio = mm_sentenced_group['total_population'].sum() / \
                         self.historical_outflows['total_population'].sum()

        # calculate standard deviation
        average_duration = np.average(self.historical_outflows.compartment_duration,
                                      weights=self.historical_outflows.total_population)
        variance = np.average((self.historical_outflows.compartment_duration - average_duration) ** 2,
                              weights=self.historical_outflows.total_population)
        std = np.sqrt(variance)

        mm_factor = affected_ratio * std

        self.apply_reduction(reduction_df=pd.DataFrame({'outflow': [outflow], 'affected_fraction': [1],
                                                        'reduction_size': [mm_factor]}),
                             reduction_type='+',
                             retroactive=retroactive)

    def chop_technical_revocations(self, technical_outflow: str, release_outflow: str = 'release',
                                   retroactive: bool = False) -> None:
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
        self.transition_dfs[df_name].loc[max_release_duration, release_outflow] += chopped_population
