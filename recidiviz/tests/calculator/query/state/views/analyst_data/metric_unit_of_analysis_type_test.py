# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests functionality of MetricUnitOfAnalysis and MetricUnitOfObservation functions"""

import re
import unittest

from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    METRIC_UNITS_OF_OBSERVATION_BY_TYPE,
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
    MetricUnitOfObservation,
    MetricUnitOfObservationType,
)


class MetricUnitOfAnalysisByTypeTest(unittest.TestCase):
    # check that index/attribute columns have no repeats
    def test_index_columns_no_repeats(self) -> None:
        for _, value in METRIC_UNITS_OF_ANALYSIS_BY_TYPE.items():
            if len(value.index_columns) != len(set(value.index_columns)):
                raise ValueError(
                    "MetricUnitOfAnalysisType `primary_key_columns` and `static_attribute_columns`"
                    " cannot have repeated/shared values."
                )

    # check that short_name only has valid character types
    def test_short_name_char_types(self) -> None:
        for unit_of_analysis_type, _ in METRIC_UNITS_OF_ANALYSIS_BY_TYPE.items():
            if not re.match(r"^\w+$", unit_of_analysis_type.short_name):
                raise ValueError(
                    "All characters in MetricUnitOfAnalysisType value must be alphanumeric or underscores."
                )


class MetricUnitOfAnalysisTest(unittest.TestCase):
    def test_get_index_columns_query_string(self) -> None:
        my_metric_aggregation_level = MetricUnitOfAnalysis(
            type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            primary_key_columns=["region_code", "my_officer_id"],
            static_attribute_columns=["my_officer_attribute"],
        )
        query_string = my_metric_aggregation_level.get_index_columns_query_string(
            prefix="my_prefix"
        )
        expected_query_string = "my_prefix.region_code, my_prefix.my_officer_id, my_prefix.my_officer_attribute"
        self.assertEqual(query_string, expected_query_string)


class MetricUnitOfObservationByTypeTest(unittest.TestCase):
    # check that short_name only has valid character types
    def test_short_name_char_types(self) -> None:
        for unit_of_observation_type, _ in METRIC_UNITS_OF_OBSERVATION_BY_TYPE.items():
            if not re.match(r"^\w+$", unit_of_observation_type.short_name):
                raise ValueError(
                    "All characters in MetricUnitOfObservationType value must be alphanumeric or underscores."
                )


class MetricUnitOfObservationTest(unittest.TestCase):
    def test_get_index_columns_query_string(self) -> None:
        my_metric_observation_level = MetricUnitOfObservation(
            type=MetricUnitOfObservationType.SUPERVISION_OFFICER,
            primary_key_columns=frozenset(["region_code", "my_officer_id"]),
        )
        query_string = my_metric_observation_level.get_primary_key_columns_query_string(
            prefix="my_prefix"
        )
        expected_query_string = "my_prefix.my_officer_id, my_prefix.region_code"
        self.assertEqual(query_string, expected_query_string)
