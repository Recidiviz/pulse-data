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
"""Unit tests for date_pairs.py."""
import unittest

from recidiviz.pipelines.ingest.pipeline_parameters import MaterializationMethod
from recidiviz.pipelines.ingest.utils import ingest_view_query_helpers


class IngestViewQueryHelpersTest(unittest.TestCase):
    """Unit tests for ingest_view_query_helpers.py."""

    def test_generate_date_bound_tuples_query(self) -> None:
        result = ingest_view_query_helpers.generate_date_bound_tuples_query(
            project_id="test-project",
            state_code="US_XX",
            raw_data_tables=["table1", "table2"],
        )
        expected = """
SELECT
    LAG(max_dt_on_date) OVER (
        ORDER BY update_date
    ) AS __lower_bound_datetime_exclusive,
    max_dt_on_date AS __upper_bound_datetime_inclusive,
FROM (
    SELECT
        update_date AS update_date,
        MAX(update_datetime) AS max_dt_on_date
    FROM (
        SELECT DISTINCT update_datetime, CAST(update_datetime AS DATE) AS update_date
        FROM `test-project.us_xx_raw_data.table1`
UNION ALL
        SELECT DISTINCT update_datetime, CAST(update_datetime AS DATE) AS update_date
        FROM `test-project.us_xx_raw_data.table2`
    )
    GROUP BY update_date
)
ORDER BY 1;"""
        self.assertEqual(result, expected)

    def test_generate_date_bound_tuples_query_latest_method(self) -> None:
        result = ingest_view_query_helpers.generate_date_bound_tuples_query(
            project_id="test-project",
            state_code="US_XX",
            raw_data_tables=["table1", "table2"],
            materialization_method=MaterializationMethod.LATEST,
        )
        expected = """
SELECT
    MAX(update_datetime) AS __upper_bound_datetime_inclusive,
    CAST(NULL AS DATETIME) AS __lower_bound_datetime_exclusive
FROM (
        SELECT DISTINCT update_datetime, CAST(update_datetime AS DATE) AS update_date
        FROM `test-project.us_xx_raw_data.table1`
UNION ALL
        SELECT DISTINCT update_datetime, CAST(update_datetime AS DATE) AS update_date
        FROM `test-project.us_xx_raw_data.table2`
);"""
        self.assertEqual(result, expected)
