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
# pylint: disable=arguments-differ, abstract-method, redefined-builtin

import apache_beam as beam


class RecidivismRateFn(beam.CombineFn):
    """Combine function that calculates the values for a recidivism rate metric.

    All inputs are either 0, representing a non-recidivism release, or 1,
    representing a recidivism release.

    This function incrementally keeps track of the number of releases and the
    sum of the inputs, representing the number of releases resulting in
    recidivism. At the end, it produces a dictionary containing values for the
    following fields:
        - total_releases: the count of all elements
        - recidivated_releases: the sum of all elements
        - recidivism_rate: the rate of recidivated releases over total releases
    """
    def create_accumulator(self):
        return (0, 0)

    def add_input(self, accumulator, input):
        (returns, releases) = accumulator

        returns += input
        releases += 1
        return returns, releases

    def merge_accumulators(self, accumulators):
        returns, releases = zip(*accumulators)
        return sum(returns), sum(releases)

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
