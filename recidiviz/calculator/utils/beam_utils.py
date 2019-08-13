# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for beam calculations."""
# pylint: disable=abstract-method, arguments-differ, redefined-builtin

import apache_beam as beam


def _add_input_for_average_metric(accumulator, input):
    """Adds input for a CombineFn that's calculating the average of a set of
    numbers."""
    (sum_of_values, number_of_values) = accumulator

    sum_of_values += input
    number_of_values += 1
    return sum_of_values, number_of_values


def _merge_accumulators_for_average_metric(accumulators):
    """Merges accumulators of a CombineFn that's calculating the average of a
    set of numbers."""
    sums_of_values, numbers_of_values = zip(*accumulators)
    return sum(sums_of_values), sum(numbers_of_values)


class RecidivismRateFn(beam.CombineFn):
    """Combine function that calculates the values for a recidivism rate metric.

    All inputs are either 0, representing a non-recidivism release, or 1,
    representing a recidivism release.

    This function incrementally keeps track of the number of releases and the
    sum of the inputs, representing the number of releases resulting in
    recidivism. At the end, it produces a dictionary containing values for the
    following fields:
        - total_releases: the count of all inputs
        - recidivated_releases: the sum of all inputs
        - recidivism_rate: the rate of recidivated releases over total releases
    """
    def create_accumulator(self):
        return (0, 0)

    def add_input(self, accumulator, input):
        return _add_input_for_average_metric(accumulator, input)

    def merge_accumulators(self, accumulators):
        return _merge_accumulators_for_average_metric(accumulators)

    def extract_output(self, accumulator):
        (returns, releases) = accumulator

        recidivism_rate = (returns + 0.0) / releases \
            if releases else float('NaN')
        output = {
            'total_releases': releases,
            'recidivated_releases': returns,
            'recidivism_rate': recidivism_rate
        }

        return output


class RecidivismLibertyFn(beam.CombineFn):
    """Combine function that calculates the values of a recidivism liberty
    metric.

    All inputs are integer values representing the number of days at liberty
    between release and reincarceration.

    This function incrementally keeps track of the number of elements
    corresponding to the key and the sum of those elements. At the end, it
    produces a dictionary containing values for the following fields:
        - returns: the count of all inputs
        - avg_liberty: the average number of days at liberty over all inputs
    """
    def create_accumulator(self):
        return (0, 0)

    def add_input(self, accumulator, input):
        return _add_input_for_average_metric(accumulator, input)

    def merge_accumulators(self, accumulators):
        return _merge_accumulators_for_average_metric(accumulators)

    def extract_output(self, sum_count):
        (sum_liberty_days, returns) = sum_count

        avg_liberty = (sum_liberty_days + 0.0) / returns \
            if returns else float('NaN')
        output = {
            'returns': returns,
            'avg_liberty': avg_liberty
        }

        return output


class SumFn(beam.CombineFn):
    """Combine function that calculates the sum of values."""
    def create_accumulator(self):
        return 0

    def add_input(self, accumulator, input):
        accumulator += input
        return accumulator

    def merge_accumulators(self, accumulators):
        return sum(accumulators)

    def extract_output(self, accumulator):
        return accumulator
