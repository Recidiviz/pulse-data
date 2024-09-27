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
"""Tests for span_query_builder.py"""
import unittest

from recidiviz.calculator.query.state.views.analyst_data.models.span_query_builder import (
    SpanQueryBuilder,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_0_super_sessions import (
    COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.observations.span_type import SpanType


class SpanQueryBuilderTest(unittest.TestCase):
    """Tests method for generating a span query builder subquery"""

    def test_generate_subquery(self) -> None:
        custom_span = SpanQueryBuilder(
            span_type=SpanType.COMPARTMENT_SESSION,
            description="This is a description of a dummy session span metric",
            sql_source=COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER.table_for_query,
            attribute_cols=["dummy_var_1", "dummy_var_2"],
            span_start_date_col="my_start",
            span_end_date_col="my_end",
        )
        expected_subquery = """
/* This is a description of a dummy session span metric */
SELECT DISTINCT
    state_code, person_id,
    "COMPARTMENT_SESSION" AS span,
    my_start AS start_date,
    my_end AS end_date,
    TO_JSON_STRING(STRUCT(
        CAST(dummy_var_1 AS STRING) AS dummy_var_1,
        CAST(dummy_var_2 AS STRING) AS dummy_var_2
    )) AS span_attributes,
FROM
    `{project_id}.sessions.compartment_level_0_super_sessions_materialized`
"""
        actual_subquery = custom_span.generate_subquery()
        self.assertEqual(expected_subquery, actual_subquery)
