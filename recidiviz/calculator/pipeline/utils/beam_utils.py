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
from typing import Any, Dict, NamedTuple

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

AverageFnResult = NamedTuple('AverageFnResult', [
        ('average_of_inputs', float),
        ('input_count', int),
        ('sum_of_inputs', int)
])


class AverageFn(beam.CombineFn):
    """Combine function that calculates the average of the input values."""
    def create_accumulator(self):
        return 0, 0

    def add_input(self, accumulator, input):
        (sum_of_inputs, input_count) = accumulator

        sum_of_inputs += input
        input_count += 1
        return sum_of_inputs, input_count

    def merge_accumulators(self, accumulators):
        sums_of_inputs, input_counts = zip(*accumulators)
        return sum(sums_of_inputs), sum(input_counts)

    def extract_output(self, sum_count):
        (sum_of_inputs, input_count) = sum_count

        avg_of_inputs = (sum_of_inputs + 0.0) / input_count if input_count else float('NaN')

        return AverageFnResult(
            average_of_inputs=avg_of_inputs,
            input_count=input_count,
            sum_of_inputs=sum_of_inputs
        )


class SumFn(beam.CombineFn):
    """Combine function that calculates the sum of the input values."""
    def create_accumulator(self):
        return 0

    def add_input(self, accumulator, input):
        accumulator += input
        return accumulator

    def merge_accumulators(self, accumulators):
        return sum(accumulators)

    def extract_output(self, accumulator):
        return accumulator


@with_input_types(beam.typehints.Dict[str, Any], str)
@with_output_types(beam.typehints.Tuple[Any, Dict[str, Any]])
class ConvertDictToKVTuple(beam.DoFn):
    """Converts a dictionary into a key value tuple by extracting a value from
     the dictionary and setting it as the key."""

    #pylint: disable=arguments-differ
    def process(self, element, key):
        key_value = element.get(key)

        if key_value:
            yield(key_value, element)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.
