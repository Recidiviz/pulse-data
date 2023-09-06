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
"""Tests for event_query_builder.py"""
import unittest

from recidiviz.calculator.query.state.views.analyst_data.models.event_query_builder import (
    EventQueryBuilder,
)
from recidiviz.calculator.query.state.views.analyst_data.models.event_type import (
    EventType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)


class EventQueryBuilderTest(unittest.TestCase):
    """Tests method for generating an event query builder subquery"""

    def test_generate_subquery(self) -> None:
        custom_event = EventQueryBuilder(
            event_type=EventType.LIBERTY_START,
            description="This is a description of a dummy liberty starts metric",
            unit_of_observation_type=MetricUnitOfAnalysisType.FACILITY,
            sql_source="""SELECT *
FROM `{project_id}.sessions.compartment_level_1_super_sessions_materialized` 
WHERE compartment_level_1 = "LIBERTY" """,
            attribute_cols=["compartment_level_1"],
            event_date_col="start_date",
        )
        expected_subquery = """
/* This is a description of a dummy liberty starts metric */
SELECT DISTINCT
    state_code, facility,
    "LIBERTY_START" AS event,
    start_date AS event_date,
    TO_JSON_STRING(STRUCT(
        CAST(compartment_level_1 AS STRING) AS compartment_level_1
    )) AS event_attributes,
FROM
    (
SELECT *
FROM `{project_id}.sessions.compartment_level_1_super_sessions_materialized` 
WHERE compartment_level_1 = "LIBERTY" 
)
"""
        actual_subquery = custom_event.generate_subquery()
        self.assertEqual(expected_subquery, actual_subquery)
