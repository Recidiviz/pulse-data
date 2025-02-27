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
"""Tests functionality of SpanSelector functions"""

import unittest

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_type import (
    SpanType,
)


class SpanSelectorTest(unittest.TestCase):
    """
    Tests SpanSelector functions
    """

    # Check that generate_span_selector_query returns intended output for attribute filters with list conditions
    def test_generate_span_selector_query_attribute_lists(self) -> None:
        self.maxDiff = None
        my_spans = SpanSelector(
            span_type=SpanType.SENTENCE_SPAN,
            span_conditions_dict={
                "any_is_drug_uniform": ["True"],
                "any_is_crime_against_person": ["False"],
            },
        )
        query_string = my_spans.generate_span_selector_query()
        expected_query_string = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.person_spans_materialized`
    WHERE span = "SENTENCE_SPAN"
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_drug_uniform") IN ("True")
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_crime_against_person") IN ("False")
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive",index_columns=["person_id", "state_code"])}
,
sub_sessions_dedup AS (
    SELECT DISTINCT
        person_id, state_code,
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
{aggregate_adjacent_spans(
    table_name='sub_sessions_dedup',
    index_columns=["person_id", "state_code"],
    attribute=[],
    end_date_field_name='end_date_exclusive')
}
"""
        self.assertEqual(query_string, expected_query_string)

    # Check that generate_span_selector_query returns intended output for the officer level population query
    def test_generate_span_selector_query_attribute_lists_officer(self) -> None:
        my_spans = SpanSelector(
            span_type=SpanType.SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION,
            span_conditions_dict={
                "custody_level": ["MAXIMUM"],
            },
        )
        query_string = my_spans.generate_span_selector_query()
        print(query_string)
        expected_query_string = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.officer_spans_materialized`
    WHERE span = "SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION"
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.custody_level") IN ("MAXIMUM")
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive", index_columns=["officer_id", "state_code"])}
,
sub_sessions_dedup AS (
    SELECT DISTINCT
        officer_id, state_code,
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
{aggregate_adjacent_spans(
    table_name='sub_sessions_dedup',
    index_columns=["officer_id", "state_code"],
    attribute=[],
    end_date_field_name='end_date_exclusive')}
"""
        self.assertEqual(query_string, expected_query_string)

    # Check that generate_span_selector_query returns intended output for attribute filters with string conditions
    def test_generate_span_selector_query_strings(self) -> None:
        my_spans = SpanSelector(
            span_type=SpanType.SENTENCE_SPAN,
            span_conditions_dict={
                "any_is_drug_uniform": 'IN ("True")',
                "any_is_crime_against_person": 'IN ("False")',
            },
        )
        query_string = my_spans.generate_span_selector_query()
        expected_query_string = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.person_spans_materialized`
    WHERE span = "SENTENCE_SPAN"
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_drug_uniform") IN ("True")
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_crime_against_person") IN ("False")
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive", index_columns=["person_id", "state_code"])}
,
sub_sessions_dedup AS (
    SELECT DISTINCT
        person_id, state_code,
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
{aggregate_adjacent_spans(
    table_name='sub_sessions_dedup',
    index_columns=["person_id", "state_code"],
    attribute=[],
    end_date_field_name='end_date_exclusive')}
"""
        self.assertEqual(query_string, expected_query_string)

    # Test query construction for spans with empty attribute filter dicts
    def test_generate_span_selector_query_empty_attribute_filters(self) -> None:
        my_spans = SpanSelector(
            span_type=SpanType.SENTENCE_SPAN,
            span_conditions_dict={},
        )
        query_string = my_spans.generate_span_selector_query()

        expected_query_string = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.person_spans_materialized`
    WHERE span = "SENTENCE_SPAN"
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive",index_columns=["person_id", "state_code"])}
,
sub_sessions_dedup AS (
    SELECT DISTINCT
        person_id, state_code,
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
{aggregate_adjacent_spans(
    table_name='sub_sessions_dedup',
    index_columns=["person_id", "state_code"],
    attribute=[],
    end_date_field_name='end_date_exclusive')}
"""
        self.assertEqual(query_string, expected_query_string)
