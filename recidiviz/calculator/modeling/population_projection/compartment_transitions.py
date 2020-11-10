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

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Dict, List
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class CompartmentTransitions(ABC):
    """Handle transition tables for one compartment that sends groups to multiple other compartments over time"""

    def __init__(self, historical_outflows: pd.DataFrame, max_sentence: int = -1):
        required_columns = ['outflow_to', 'compartment', 'compartment_duration', 'total_population']
        missing_columns = [col for col in required_columns if col not in historical_outflows.columns]
        if len(missing_columns) != 0:
            raise ValueError(f"historical_outflows dataframe is missing the required columns {required_columns}")

        if 'remaining' in historical_outflows['outflow_to'].unique():
            raise ValueError("historical_outflows dataframe cannot contain an outflow column named `remaining`")

        self.max_sentence = max_sentence

        self.outflows = historical_outflows['outflow_to'].unique()

        self.historical_outflows = historical_outflows

        self.transition_dfs: Dict[str, Any] = {}

        self.long_sentence_transitions = pd.DataFrame()

    def initialize_transition_table(self, max_sentence: int = -1):
        """Populate the 'before' transition table. Optionally accepts a max sentence length after which sentences are
        grouped into long-sentence bucket. If no max_setence is passed, calculates the 98% stay duration threshold"""
        if self.historical_outflows.empty:
            raise ValueError("Cannot create a transition table with an empty transitions_data dataframe")
        for column in ['total_population', 'compartment_duration']:
            if any(self.historical_outflows[column] < 0):
                negative_rows = self.historical_outflows[self.historical_outflows[column] < 0]
                raise ValueError(f"Transition data '{column}' column cannot contain negative values {negative_rows}")

        if max_sentence <= 0:
            if self.historical_outflows.empty:
                # TODO(#4512): do not allow people to create an empty model
                self.max_sentence = 0
            elif len(self.historical_outflows) == 1:
                self.max_sentence = int(max(self.historical_outflows['compartment_duration']))
            else:
                threshold_percentile = 0.98
                self._set_max_sentence_from_threshold(threshold_percentile)
        else:
            self.max_sentence = max_sentence

        self.transition_dfs = {
            'before': {outflow: np.zeros(self.max_sentence + 1) for outflow in self.outflows},
            'transitory': 0,
            'after_retroactive': 0,
            'after_non_retroactive': 0
        }

        self._generate_transition_tables(state='before')

    def _set_max_sentence_from_threshold(self, threshold_percentile: float):
        """Set the maximum sentence length as a clipped value from a threshold percentile"""

        if (threshold_percentile < 0) | (threshold_percentile > 1):
            raise ValueError(f"Threshold percentage {threshold_percentile} must be a percentage between 0-1")

        # If the threshold percentile is 100% then return the max
        if threshold_percentile == 1:
            self.max_sentence = int(np.ceil(max(self.historical_outflows['compartment_duration'])))

        # Otherwise, compute the max sentence using the threshold_percentile over the total population
        total_transitions = sum(self.historical_outflows['total_population'])

        ordered_transitions = \
            self.historical_outflows.sort_values(by='compartment_duration').reset_index(drop=True)

        # Compute the percentile based on total population
        transitions_above_threshold_slice = (
                ordered_transitions[['compartment_duration', 'total_population']].cumsum() >=
                threshold_percentile * total_transitions
        )
        # Apply the boolean map to get threshold index
        max_sentence_index = \
            min(ordered_transitions[transitions_above_threshold_slice['total_population']].index)
        self.max_sentence = int(np.ceil(ordered_transitions.loc[max_sentence_index]['compartment_duration']))

    def _generate_transition_tables(self, state: str, historical_outflows=None):
        """
        Accepts a state for which to populate transition probabilities
        optionally accepts a DataFrame with alternative transition data to use
        """
        if self.max_sentence <= 0:
            raise ValueError("Max sentence length is not set")

        if historical_outflows is None:
            historical_outflows = self.historical_outflows

        historical_outflows_short = historical_outflows[historical_outflows.compartment_duration <= self.max_sentence]
        historical_outflows_long = historical_outflows[historical_outflows.compartment_duration > self.max_sentence]

        sentence_length = historical_outflows['compartment_duration'].apply(np.ceil)
        short_sentence_length = sentence_length[sentence_length <= self.max_sentence]
        long_sentence_length = sentence_length[sentence_length > self.max_sentence]

        grouped_short_outflows = historical_outflows_short.groupby([short_sentence_length,
                                                                    historical_outflows_short.outflow_to])
        grouped_long_outflows = historical_outflows_long.groupby([long_sentence_length,
                                                                  historical_outflows_long.outflow_to])

        self.transition_dfs[state] = grouped_short_outflows['total_population'].sum().unstack().reindex(
            range(1, self.max_sentence + 1)).fillna(0).to_dict(
            orient='list')

        # Track life years of the lump category if handling 'before' state
        if state == 'before':
            if historical_outflows_long.empty:
                self.long_sentence_transitions = None
            else:
                self.long_sentence_transitions = grouped_long_outflows['total_population'].sum().unstack().reindex(
                    range(self.max_sentence + 1, int(long_sentence_length.max()) + 1)).fillna(0).to_dict(orient='list')

    @abstractmethod
    def normalize_long_sentences(self):
        pass

    def normalize_transitions(self, state: str, before_table=None):
        """Convert the per-ts population counts into normalized probabilities"""
        if (self.transition_dfs is None) or (state not in self.transition_dfs) or (self.transition_dfs[state] == 0):
            raise ValueError("Cannot normalize transition tables before they are initialized")

        if 'remaining' in self.transition_dfs[state]:
            return

        if before_table is None:
            before_table = self.transition_dfs[state]

        if 'remaining' in before_table:
            raise ValueError("`normalize_transitions` cannot be called with a normalized `before_table`")

        if self.max_sentence <= 0:
            raise ValueError("Max sentence length is not set")

        normalized_df = deepcopy(self.transition_dfs[state])
        after_table = self.transition_dfs[state]

        before_counted = 0
        after_counted = 0

        # if no long sentence data, no need to include it in totals
        if self.long_sentence_transitions is None:
            before_total = sum([sum(before_table[outflow]) for outflow in before_table])
            after_total = sum([sum(after_table[outflow]) for outflow in after_table])

        else:
            before_total = sum([sum(before_table[outflow]) for outflow in before_table]) + \
                           sum([sum(self.long_sentence_transitions[outflow])
                                for outflow in self.long_sentence_transitions])

            after_total = sum([sum(after_table[outflow]) for outflow in after_table]) + \
                          sum([sum(self.long_sentence_transitions[outflow])
                               for outflow in self.long_sentence_transitions])

        for sentence_len in range(self.max_sentence):

            if before_counted == before_total:
                for outflow in normalized_df:
                    normalized_df[outflow][sentence_len] = 0
                continue

            after_counted += sum([after_table[outflow][sentence_len] for outflow in after_table])

            ts_release_rate = 1 - (1 - after_counted / after_total) / (1 - before_counted / before_total)
            ts_released = sum([after_table[outflow][sentence_len] for outflow in after_table])

            if ts_released > 0:
                for outflow in normalized_df:
                    normalized_df[outflow][sentence_len] *= ts_release_rate / ts_released

            before_counted += sum([before_table[outflow][sentence_len] for outflow in before_table])

        self.transition_dfs[state] = normalized_df

        self.transition_dfs[state]['remaining'] = [0 for i in range(self.max_sentence)]
        for ts in range(self.max_sentence):
            self.transition_dfs[state]['remaining'][ts] = \
                1 - sum([self.transition_dfs[state][i][ts] for i in self.transition_dfs[state]])

        self.transition_dfs[state] = pd.DataFrame(self.transition_dfs[state], index=range(1, self.max_sentence + 1))

        self.transition_dfs[state].remaining = self.transition_dfs[state].remaining.apply(lambda x: round(x, 8))

        # Make sure all transition probabilities are between 0-1
        for compartment in self.transition_dfs[state]:
            if any((self.transition_dfs[state][compartment] < 0) | (self.transition_dfs[state][compartment] > 1)):
                raise ValueError(f"'{compartment}' transition has probabilities out of bounds for state '{state}':\n"
                                 f"{self.transition_dfs[state][compartment]}")

    def initialize(self, compartment_policies: List[SparkPolicy]):
        """initialize all transition tables given a list of SparkPolicy. This is the only initializing function that
        should get called outside the object"""
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
        if any([list(before_after_retroactive[i]) !=
                list(self.transition_dfs['after_retroactive'][i])[:len(before_after_retroactive[i])]
                for i in self.transition_dfs['after_retroactive']]):
            raise ValueError("Policy function was applied to the wrong transition state")

        # generate the transitory table for retroactive policies
        self.transition_dfs['transitory'] = deepcopy(self.transition_dfs['after_retroactive'])

        # standardize the transition probabilities for all transition classes
        self.normalize_transitions('transitory', before_table=self.transition_dfs['before'])
        self.normalize_transitions('before')
        self.normalize_transitions('after_retroactive')
        self.normalize_transitions('after_non_retroactive')
        self.normalize_long_sentences()

    def get_per_ts_transition_table(self, current_ts: int, policy_ts: int):
        short_len_transitions = self._get_per_ts_transition_table(current_ts, policy_ts)
        return short_len_transitions, self.long_sentence_transitions

    def _get_per_ts_transition_table(self, current_ts: int, policy_ts: int):
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

    def __eq__(self, other):
        """Only works for initialized transition tables"""
        for state in self.transition_dfs:
            if set(self.transition_dfs[state].columns) != set(other.transition_dfs[state].columns):
                return False
            for outflow in self.transition_dfs[state]:
                if not all(self.transition_dfs[state][outflow] == other.transition_dfs[state][outflow]):
                    return False
        return True

    @staticmethod
    def get_df_name_from_retroactive(retroactive: bool):
        if retroactive:
            return 'after_retroactive'

        return 'after_non_retroactive'

    def test_non_retroactive_policy(self):
        self.transition_dfs['after_non_retroactive']['jail'] = \
            [0] * len(self.transition_dfs['after_non_retroactive']['jail'])

    def test_retroactive_policy(self):
        self.transition_dfs['after_retroactive']['jail'] = \
            [0] * len(self.transition_dfs['after_retroactive']['jail'])

    def extend_tables(self, new_max_sentence: int):
        """
        extends the max sentence of transition tables to `new_max_sentence` --> doesn't handle long-sentences
            correctly but those should get taken out anyway
        doesn't shorten max sentence if new_max_sentence is smaller than self.max_sentence
        """
        # Ensure none of the transition tables have been normalized before they are extended
        for state in self.transition_dfs:
            if self.transition_dfs[state] != 0:
                self.check_table_invariant_before_normalization(state)

        if new_max_sentence <= self.max_sentence:
            return

        for state in self.transition_dfs:
            # skip over un-populated tables
            if self.transition_dfs[state] != 0:
                self.transition_dfs[state] = {
                    outflow: (self.transition_dfs[state][outflow] +
                              [0 for ts in range(new_max_sentence - self.max_sentence)])
                    for outflow in self.transition_dfs[state]
                }

        self.max_sentence = new_max_sentence

    def unnormalize_table(self, state: str):
        """revert a normalized table back to an un-normalized dict. sum of all total populations will be 1"""
        if 'remaining' not in self.transition_dfs[state]:
            raise ValueError("trying to unnormalize a table that isn't normalized")

        for sentence_length in self.transition_dfs[state].index.sort_values(ascending=False):
            for shorter_sentence in range(1, sentence_length):
                self.transition_dfs[state].loc[sentence_length] *= \
                    self.transition_dfs[state].loc[shorter_sentence].drop('remaining').sum()

        self.transition_dfs[state] = self.transition_dfs[state].drop('remaining', axis=1).to_dict()

    def use_alternate_transitions_data(self, alternate_historical_transitions: pd.DataFrame, retroactive: bool):
        """Replace the historical admission data for this specific group with another data from a different set"""

        self.extend_tables(min([30 * 12, int(np.ceil(max(alternate_historical_transitions.compartment_duration)))]))
        df_name = self.get_df_name_from_retroactive(retroactive)
        self._generate_transition_tables(df_name, alternate_historical_transitions)

    def preserve_normalized_outflow_behavior(self, outflows: List[str], state: str, before_state: str = 'before'):
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

    def check_table_invariant_after_normalization(self, state: str):
        """Make sure transition lists are all positive decimal probability values for the provided transition state"""
        for outflow, probabilities in self.transition_dfs[state].items():
            if any(probabilities < 0) or any(probabilities > 1):
                raise ValueError(f"All `{state}` probabilities for outflow `{outflow}` must be between 0 and 1\n"
                                 f"{probabilities}")

    def apply_reduction(self, reduction_dict: Dict[str, float], reduction_type: str, retroactive: bool = False):
        """
        scale down outflow compartment_duration distributions either multiplicatively or additively
        NOTE: does not change other outflows, ie their normalized values will be different!
        `reduction_dict` should be a dict of affected outflows and their corresponding reduction
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

        for outflow, reduction in reduction_dict.items():
            for sentence_length in range(2, self.max_sentence + 1):
                # record population to re-distribute
                sentence_count = self.transition_dfs[df_name][outflow][sentence_length - 1]

                # start by clearing df entry that's getting re-distributed
                self.transition_dfs[df_name][outflow][sentence_length - 1] = 0

                # calculate new sentence length
                if reduction_type == '*':
                    new_sentence_length = max([sentence_length * (1 - reduction), 1])
                elif reduction_type == '+':
                    new_sentence_length = max([sentence_length - reduction, 1])

                # separate out non-integer sentence length into one chunk rounded up and one chunk rounded down,
                #   weighted by where in the middle actual sentence falls
                longer_bit = (new_sentence_length % 1) * sentence_count
                shorter_bit = sentence_count - longer_bit

                # add in new sentence length probabilities to the df
                self.transition_dfs[df_name][outflow][int(new_sentence_length) - 1] += shorter_bit
                self.transition_dfs[df_name][outflow][int(new_sentence_length)] += longer_bit

    def reallocate_outflow(self, outflow: str, new_outflow: str, retroactive: bool = False):
        """
        reallocate outflow from `outflow` to `new_outflow`. If `new_outflow` doesn't exist, create a new column for it.
        """
        # ensure tables haven't been normalized already
        if 'remaining' in self.transition_dfs['before']:
            raise ValueError("Outflow cannot be reallocated on a normalized transition table")

        df_name = self.get_df_name_from_retroactive(retroactive)

        new_outflow_value = self.transition_dfs[df_name].get(new_outflow, 0) + self.transition_dfs[outflow]
        self.transition_dfs[df_name][new_outflow] = new_outflow_value
        del self.transition_dfs[df_name][outflow]
