# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests functionality of MetricPopulationType functions"""

import re
import unittest

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulation,
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.person_span_type import (
    PersonSpanType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.person_spans import (
    PERSON_SPANS_BY_TYPE,
)


class MetricPopulationsByTypeTest(unittest.TestCase):
    # check that population_type = key value
    def test_population_type_matches_key(self) -> None:
        for key, value in METRIC_POPULATIONS_BY_TYPE.items():
            self.assertEqual(
                value.population_type,
                key,
                "MetricPopulation `population_type` does not match key value.",
            )

    # check that population_name_short only has valid character types
    def test_population_name_short_char_types(self) -> None:
        for _, value in METRIC_POPULATIONS_BY_TYPE.items():
            if not re.match(r"^\w+$", value.population_name_short):
                raise ValueError(
                    "All characters in MetricPopulationType value must be alphanumeric or underscores."
                )

    # check that all attribute filters are compatible with person spans in configured MetricPopulation
    def test_compatible_person_span_filters(self) -> None:
        for _, value in METRIC_POPULATIONS_BY_TYPE.items():
            for span_type, attribute_dict in value.person_spans_dict.items():
                for attribute in attribute_dict:
                    if attribute not in PERSON_SPANS_BY_TYPE[span_type].attribute_cols:
                        raise ValueError(
                            f"Span attribute `{attribute}` is not supported by {span_type.value} span. "
                            f"Supported attributes: {PERSON_SPANS_BY_TYPE[span_type].attribute_cols}"
                        )


class MetricPopulationTest(unittest.TestCase):
    """
    Tests MetricPopulation functions
    """

    # Check that get_population_query returns intended output for attribute filters with list conditions
    def test_get_population_query_attribute_lists(self) -> None:
        my_population = MetricPopulation(
            population_type=MetricPopulationType.CUSTOM,
            person_spans_dict={
                PersonSpanType.SENTENCE_SPAN: {
                    "any_is_drug_uniform": ["True"],
                    "any_is_crime_against_person": ["False"],
                },
                PersonSpanType.CUSTODY_LEVEL_SESSION: {
                    "custody_level": ["MAXIMUM"],
                },
            },
        )
        query_string = my_population.get_population_query()
        expected_query_string = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.person_spans_materialized`
    WHERE (span = "SENTENCE_SPAN"
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_drug_uniform") IN ("True")
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_crime_against_person") IN ("False"))
    OR (span = "CUSTODY_LEVEL_SESSION"
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.custody_level") IN ("MAXIMUM"))
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive",index_columns=["state_code", "person_id"])}
,
# Get all span types represented by a single sub-sessionized span of time
sub_sessions_deduped AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        ARRAY_AGG(span) AS span_types,
    FROM
        sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
)
# Filter to only sub-sessions where all span types are covered, indicating that all span filter conditions are met.
SELECT * EXCEPT(span_types) FROM sub_sessions_deduped
WHERE (
    SELECT LOGICAL_AND(
        span IN UNNEST(["CUSTODY_LEVEL_SESSION", "SENTENCE_SPAN"])
    ) FROM UNNEST(span_types) 
span)
"""
        self.assertEqual(query_string, expected_query_string)

    # Check that get_population_query returns intended output for attribute filters with string conditions
    def test_get_population_query_strings(self) -> None:
        my_population = MetricPopulation(
            population_type=MetricPopulationType.CUSTOM,
            person_spans_dict={
                PersonSpanType.SENTENCE_SPAN: {
                    "any_is_drug_uniform": 'IN ("True")',
                    "any_is_crime_against_person": 'IN ("False")',
                },
                PersonSpanType.CUSTODY_LEVEL_SESSION: {
                    "custody_level": 'IN ("MAXIMUM")',
                },
            },
        )
        query_string = my_population.get_population_query()
        expected_query_string = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.person_spans_materialized`
    WHERE (span = "SENTENCE_SPAN"
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_drug_uniform") IN ("True")
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.any_is_crime_against_person") IN ("False"))
    OR (span = "CUSTODY_LEVEL_SESSION"
        AND JSON_EXTRACT_SCALAR(span_attributes, "$.custody_level") IN ("MAXIMUM"))
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive", index_columns=["state_code", "person_id"])}
,
# Get all span types represented by a single sub-sessionized span of time
sub_sessions_deduped AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        ARRAY_AGG(span) AS span_types,
    FROM
        sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
)
# Filter to only sub-sessions where all span types are covered, indicating that all span filter conditions are met.
SELECT * EXCEPT(span_types) FROM sub_sessions_deduped
WHERE (
    SELECT LOGICAL_AND(
        span IN UNNEST(["CUSTODY_LEVEL_SESSION", "SENTENCE_SPAN"])
    ) FROM UNNEST(span_types) 
span)
"""
        self.assertEqual(query_string, expected_query_string)

    # Test query construction for person spans with empty attribute filter dicts
    def test_get_population_query_empty_attribute_filters(self) -> None:
        my_population = MetricPopulation(
            population_type=MetricPopulationType.CUSTOM,
            person_spans_dict={
                PersonSpanType.SENTENCE_SPAN: {},
                PersonSpanType.CUSTODY_LEVEL_SESSION: {},
            },
        )
        query_string = my_population.get_population_query()
        expected_query_string = f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.person_spans_materialized`
    WHERE (span = "SENTENCE_SPAN")
    OR (span = "CUSTODY_LEVEL_SESSION")
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive",index_columns=["state_code", "person_id"])}
,
# Get all span types represented by a single sub-sessionized span of time
sub_sessions_deduped AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        ARRAY_AGG(span) AS span_types,
    FROM
        sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
)
# Filter to only sub-sessions where all span types are covered, indicating that all span filter conditions are met.
SELECT * EXCEPT(span_types) FROM sub_sessions_deduped
WHERE (
    SELECT LOGICAL_AND(
        span IN UNNEST(["CUSTODY_LEVEL_SESSION", "SENTENCE_SPAN"])
    ) FROM UNNEST(span_types) 
span)
"""
        self.assertEqual(query_string, expected_query_string)
