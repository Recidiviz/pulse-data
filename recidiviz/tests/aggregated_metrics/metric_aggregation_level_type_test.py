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
"""Tests functionality of MetricAggregationLevel functions"""

import re
import unittest

from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    METRIC_AGGREGATION_LEVELS_BY_TYPE,
    MetricAggregationLevel,
    MetricAggregationLevelType,
)


class MetricAggregationLevelsByTypeTest(unittest.TestCase):
    # check that index columns have no repeats
    def test_index_columns_no_repeats(self) -> None:
        for _, value in METRIC_AGGREGATION_LEVELS_BY_TYPE.items():
            if len(value.index_columns) != len(set(value.index_columns)):
                raise ValueError(
                    "MetricAggregationLevelType `primary_key_columns` and `attribute_columns`"
                    " cannot have repeated/shared values."
                )

    # check that level_type = key value
    def test_level_type_matches_key(self) -> None:
        for key, value in METRIC_AGGREGATION_LEVELS_BY_TYPE.items():
            self.assertEqual(
                value.level_type,
                key,
                "MetricAggregationLevel `level_type` does not match key value.",
            )

    # check that level_name_short only has valid character types
    def test_level_name_short_char_types(self) -> None:
        for _, value in METRIC_AGGREGATION_LEVELS_BY_TYPE.items():
            if not re.match(r"^\w+$", value.level_name_short):
                raise ValueError(
                    "All characters in MetricAggregationLevelType value must be alphanumeric or underscores."
                )


class MetricAggregationLevelTest(unittest.TestCase):
    def test_get_index_columns_query_string(self) -> None:
        my_metric_aggregation_level = MetricAggregationLevel(
            level_type=MetricAggregationLevelType.SUPERVISION_OFFICER,
            client_assignment_query="SELECT * FROM `{project_id}.{my_dataset}.my_table`",
            primary_key_columns=["region_code", "my_officer_id"],
            attribute_columns=["my_officer_attribute"],
            dataset_kwargs={"my_dataset": "custom_dataset"},
        )
        query_string = my_metric_aggregation_level.get_index_columns_query_string(
            prefix="my_prefix"
        )
        expected_query_string = "my_prefix.region_code, my_prefix.my_officer_id, my_prefix.my_officer_attribute"
        self.assertEqual(query_string, expected_query_string)
