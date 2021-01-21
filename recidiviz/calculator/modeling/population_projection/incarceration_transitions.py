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
"""CompartmentTransitions instance with slow trickle long-sentence behavior"""

import pandas as pd
from numpy import average, sqrt, isinf
from recidiviz.calculator.modeling.population_projection.compartment_transitions import CompartmentTransitions


class IncarceratedTransitions(CompartmentTransitions):
    """Encapsulate the transition logic for incarceration and supervision scenarios"""

    def normalize_long_sentences(self):
        # Note: this assumes policies cannot affect long-sentence inmates.
        # If we implement policies that do, this step will need to be moved to before policies are applied.

        # if no data, just populate 'remaining' (it'll run but doesn't actually matter)
        if self.long_sentence_transitions.empty or self.long_sentence_transitions.sum().sum() == 0:
            self.long_sentence_transitions = pd.DataFrame({'remaining': [1]})
            return

        lst = self.long_sentence_transitions

        # calculate the fraction of total long-sentence releases corresponding to each outflow
        outflow_ratios = lst.sum(axis=0) / lst.sum().sum()

        # calculate average remaining sentence by taking weighted sum of number of sentence * sentence length
        avg_sentences = (lst.mul(lst.index, axis=0) / lst.sum(axis=0)).sum(axis=0)

        # take long_sentence transitions to be 1 / avg_sentence, scaled by outflow fraction
        long_sentence_transitions = ((1 / avg_sentences) * outflow_ratios).fillna(0)
        long_sentence_transitions[isinf(long_sentence_transitions)] = 0

        # convert to DataFrame
        long_sentence_transitions = pd.DataFrame(long_sentence_transitions).transpose()

        # as always, `remaining` probability is just 1 - everything else
        long_sentence_transitions['remaining'] = [1 - long_sentence_transitions.iloc[0].sum()]

        self.long_sentence_transitions = long_sentence_transitions

    def abolish_mandatory_minimum(self, current_mm: float, outflow: str, retroactive: bool = False):

        mm_sentenced_group = self.historical_outflows[self.historical_outflows.compartment_duration == current_mm]
        # Do not modify the transition table if there are no sentences at the mandatory minimum
        if len(mm_sentenced_group) == 0:
            return

        affected_ratio = mm_sentenced_group['total_population'].sum()/self.historical_outflows['total_population'].sum()

        # calculate standard deviation
        average_duration = average(self.historical_outflows.compartment_duration,
                                   weights=self.historical_outflows.total_population)
        variance = average((self.historical_outflows.compartment_duration - average_duration) ** 2,

                           weights=self.historical_outflows.total_population)
        std = sqrt(variance)

        mm_factor = affected_ratio * std

        self.apply_reduction(reduction_df=pd.DataFrame({'outflow': [outflow], 'affected_fraction': [1],
                                                        'reduction_size': [mm_factor]}),
                             reduction_type='+',
                             retroactive=retroactive)
