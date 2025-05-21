# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for inverted_task_criteria_big_query_view_builder.py"""
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

CRITERIA_1_STATE_AGNOSTIC = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name="MY_STATE_AGNOSTIC_CRITERIA",
    description="A state-agnostic criteria",
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.my_sessions_materialized`",
    meets_criteria_default=True,
    reasons_fields=[],
)

CRITERIA_2_STATE_AGNOSTIC = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name="ANOTHER_STATE_AGNOSTIC_CRITERIA",
    description="Another state-agnostic criteria",
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="fees_owed",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Amount of fees owed",
        ),
    ],
)

CRITERIA_3_STATE_SPECIFIC = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name="US_KY_CRITERIA_5",
    description="Another criteria for KY residents",
    state_code=StateCode.US_KY,
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.drug_tests` "
    "WHERE state_code = 'US_KY'",
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="fees_owed",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Amount of fees owed",
        ),
        ReasonsField(
            name="offense_types",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Offense types that person is serving",
        ),
    ],
)


class TestInvertedTaskCriteriaBigQueryViewBuilder(BigQueryEmulatorTestCase):
    """Tests for the InvertedTaskCriteriaBigQueryViewBuilder."""

    def test_inverted_criteria_state_specific_criteria_name(self) -> None:
        """Checks inverted state-specific criteria"""
        criteria = StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_3_STATE_SPECIFIC
        )
        self.assertEqual(criteria.criteria_name, "US_KY_NOT_CRITERIA_5")

        self.assertEqual(StateCode.US_KY, criteria.state_code)

        self.assertEqual(
            criteria.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ky",
                table_id="not_criteria_5_materialized",
            ),
        )

        self.assertEqual(criteria.meets_criteria_default, False)

        # Check that the reasons fields are the same between the inverted criteria view builder and the sub-criteria
        self.assertEqual(criteria.reasons_fields, criteria.sub_criteria.reasons_fields)

        # Check that query template is correct
        expected_query_template = """
WITH criteria_query_base AS (

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,
    SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64) AS fees_owed,
    JSON_VALUE_ARRAY(reason_v2, '$.offense_types') AS offense_types,
FROM
    `{project_id}.task_eligibility_criteria_us_ky.criteria_5_materialized`
)
,
_pre_sessionized AS
(
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    TO_JSON(STRUCT(
        fees_owed,
        offense_types
    )) AS reason_v2,
FROM
    criteria_query_base
)
,
_aggregated AS
(

SELECT
    person_id, state_code,
    -- Recalculate session-ids after aggregation
    ROW_NUMBER() OVER (
        PARTITION BY person_id, state_code 
        ORDER BY start_date, IFNULL(end_date, "9999-12-31"),CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2)
    ) AS session_id,
    date_gap_id,
    start_date,
    end_date,
    meets_criteria, reason, reason_v2
FROM (
    SELECT
        person_id, state_code,
        session_id,
        date_gap_id,
        MIN(start_date) OVER w AS start_date,
        IF(MAX(end_date) OVER w = "9999-12-31", NULL, MAX(end_date) OVER w) AS end_date,
        meets_criteria, reason, reason_v2
    FROM
        (
        SELECT 
            *,
            SUM(IF(session_boundary, 1, 0)) OVER (PARTITION BY person_id, state_code,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2) ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS session_id,
            SUM(IF(date_gap, 1, 0)) OVER (PARTITION BY person_id, state_code ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS date_gap_id,
        FROM
            (
            SELECT
                person_id, state_code,
                start_date,
                IFNULL(end_date, "9999-12-31") AS end_date,
                -- Define a session boundary if there is no prior adjacent span with the same attribute columns
                COALESCE(LAG(end_date) OVER (PARTITION BY person_id, state_code,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2) ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS session_boundary,
                -- Define a date gap if there is no prior adjacent span, regardless of attribute columns
                COALESCE(LAG(end_date) OVER (PARTITION BY person_id, state_code ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS date_gap,
                meets_criteria, reason, reason_v2
            FROM _pre_sessionized
            )
        )
        -- TODO(goccy/go-zetasqlite#123): Workaround emulator unsupported QUALIFY without WHERE/HAVING/GROUP BY clause
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER w = 1
        WINDOW w AS (PARTITION BY person_id, state_code, session_id,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2))
    )

)
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    reason_v2
FROM _aggregated
WHERE state_code = 'US_KY'
"""
        self.assertEqual(expected_query_template, criteria.view_query_template)

    def test_inverted_criteria_state_agnostic_criteria_name(self) -> None:
        """Checks inverted state-agnostic criteria"""
        criteria = StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_2_STATE_AGNOSTIC,
            invert_meets_criteria_default=False,
        )
        self.assertEqual(criteria.criteria_name, "NOT_ANOTHER_STATE_AGNOSTIC_CRITERIA")

        self.assertEqual(
            criteria.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_general",
                table_id="not_another_state_agnostic_criteria_materialized",
            ),
        )

        self.assertEqual(criteria.meets_criteria_default, False)

        # Check that the reasons fields are the same between the inverted criteria view builder and the sub-criteria
        self.assertEqual(criteria.reasons_fields, criteria.sub_criteria.reasons_fields)

        # Check that query template is correct
        expected_query_template = """
WITH criteria_query_base AS (

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,
    SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64) AS fees_owed,
FROM
    `{project_id}.task_eligibility_criteria_general.another_state_agnostic_criteria_materialized`
)
,
_pre_sessionized AS
(
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    TO_JSON(STRUCT(
        fees_owed
    )) AS reason_v2,
FROM
    criteria_query_base
)
,
_aggregated AS
(

SELECT
    person_id, state_code,
    -- Recalculate session-ids after aggregation
    ROW_NUMBER() OVER (
        PARTITION BY person_id, state_code 
        ORDER BY start_date, IFNULL(end_date, "9999-12-31"),CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2)
    ) AS session_id,
    date_gap_id,
    start_date,
    end_date,
    meets_criteria, reason, reason_v2
FROM (
    SELECT
        person_id, state_code,
        session_id,
        date_gap_id,
        MIN(start_date) OVER w AS start_date,
        IF(MAX(end_date) OVER w = "9999-12-31", NULL, MAX(end_date) OVER w) AS end_date,
        meets_criteria, reason, reason_v2
    FROM
        (
        SELECT 
            *,
            SUM(IF(session_boundary, 1, 0)) OVER (PARTITION BY person_id, state_code,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2) ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS session_id,
            SUM(IF(date_gap, 1, 0)) OVER (PARTITION BY person_id, state_code ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS date_gap_id,
        FROM
            (
            SELECT
                person_id, state_code,
                start_date,
                IFNULL(end_date, "9999-12-31") AS end_date,
                -- Define a session boundary if there is no prior adjacent span with the same attribute columns
                COALESCE(LAG(end_date) OVER (PARTITION BY person_id, state_code,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2) ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS session_boundary,
                -- Define a date gap if there is no prior adjacent span, regardless of attribute columns
                COALESCE(LAG(end_date) OVER (PARTITION BY person_id, state_code ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS date_gap,
                meets_criteria, reason, reason_v2
            FROM _pre_sessionized
            )
        )
        -- TODO(goccy/go-zetasqlite#123): Workaround emulator unsupported QUALIFY without WHERE/HAVING/GROUP BY clause
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER w = 1
        WINDOW w AS (PARTITION BY person_id, state_code, session_id,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2))
    )

)
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    reason_v2
FROM _aggregated
"""

        self.assertEqual(expected_query_template, criteria.view_query_template)

    def test_inverted_criteria_empty_reasons(self) -> None:
        """Checks inverted criteria with an empty reasons list"""
        criteria = StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_1_STATE_AGNOSTIC
        )
        self.assertEqual(criteria.criteria_name, "NOT_MY_STATE_AGNOSTIC_CRITERIA")

        self.assertEqual(
            criteria.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_general",
                table_id="not_my_state_agnostic_criteria_materialized",
            ),
        )

        self.assertEqual(criteria.meets_criteria_default, False)

        # Check that the reasons fields are the same between the inverted criteria view builder and the sub-criteria
        self.assertEqual(criteria.reasons_fields, criteria.sub_criteria.reasons_fields)

        # Check that query template is correct
        expected_query_template = """
WITH criteria_query_base AS (

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,

FROM
    `{project_id}.task_eligibility_criteria_general.my_state_agnostic_criteria_materialized`
)
,
_pre_sessionized AS
(
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    TO_JSON(STRUCT()) AS reason_v2,
FROM
    criteria_query_base
)
,
_aggregated AS
(

SELECT
    person_id, state_code,
    -- Recalculate session-ids after aggregation
    ROW_NUMBER() OVER (
        PARTITION BY person_id, state_code 
        ORDER BY start_date, IFNULL(end_date, "9999-12-31"),CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2)
    ) AS session_id,
    date_gap_id,
    start_date,
    end_date,
    meets_criteria, reason, reason_v2
FROM (
    SELECT
        person_id, state_code,
        session_id,
        date_gap_id,
        MIN(start_date) OVER w AS start_date,
        IF(MAX(end_date) OVER w = "9999-12-31", NULL, MAX(end_date) OVER w) AS end_date,
        meets_criteria, reason, reason_v2
    FROM
        (
        SELECT 
            *,
            SUM(IF(session_boundary, 1, 0)) OVER (PARTITION BY person_id, state_code,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2) ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS session_id,
            SUM(IF(date_gap, 1, 0)) OVER (PARTITION BY person_id, state_code ORDER BY start_date, IFNULL(end_date, "9999-12-31")) AS date_gap_id,
        FROM
            (
            SELECT
                person_id, state_code,
                start_date,
                IFNULL(end_date, "9999-12-31") AS end_date,
                -- Define a session boundary if there is no prior adjacent span with the same attribute columns
                COALESCE(LAG(end_date) OVER (PARTITION BY person_id, state_code,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2) ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS session_boundary,
                -- Define a date gap if there is no prior adjacent span, regardless of attribute columns
                COALESCE(LAG(end_date) OVER (PARTITION BY person_id, state_code ORDER BY start_date, IFNULL(end_date, "9999-12-31")) != start_date, TRUE) AS date_gap,
                meets_criteria, reason, reason_v2
            FROM _pre_sessionized
            )
        )
        -- TODO(goccy/go-zetasqlite#123): Workaround emulator unsupported QUALIFY without WHERE/HAVING/GROUP BY clause
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER w = 1
        WINDOW w AS (PARTITION BY person_id, state_code, session_id,CAST(meets_criteria AS STRING) ,TO_JSON_STRING(reason) ,TO_JSON_STRING(reason_v2))
    )

)
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    reason_v2
FROM _aggregated
"""

        self.assertEqual(expected_query_template, criteria.view_query_template)

    def test_inverted_criteria_state_specific_non_inverted_meets_default(self) -> None:
        """Checks inverted state-specific criteria with non-inverted meets_criteria_default"""
        criteria = StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_3_STATE_SPECIFIC,
            invert_meets_criteria_default=False,
        )

        self.assertEqual(criteria.meets_criteria_default, True)
