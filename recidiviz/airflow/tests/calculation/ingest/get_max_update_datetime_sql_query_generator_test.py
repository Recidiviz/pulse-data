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
"""Unit tests for GetMaxUpdateDateTimeSqlQueryGenerator"""
import datetime
import unittest
from unittest.mock import Mock, create_autospec, patch

import freezegun
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from recidiviz.airflow.dags.calculation.ingest.get_max_update_datetime_sql_query_generator import (
    GetMaxUpdateDateTimeSqlQueryGenerator,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class TestGetMaxUpdateDateTimeSqlQueryGenerator(unittest.TestCase):
    """Unit tests for GetWatermarkSqlQueryGenerator"""

    maxDiff = None

    def setUp(self) -> None:
        self.raw_data_import_dag_enabled_patcher = patch(
            "recidiviz.airflow.dags.calculation.ingest.get_max_update_datetime_sql_query_generator.is_raw_data_import_dag_enabled"
        )
        self.raw_data_import_dag_enabled_mock = (
            self.raw_data_import_dag_enabled_patcher.start()
        )
        self.raw_data_import_dag_enabled_mock.side_effect = self._gating

    def tearDown(self) -> None:
        self.raw_data_import_dag_enabled_patcher.stop()

    @staticmethod
    def _gating(state_code: StateCode, raw_data_instance: DirectIngestInstance) -> bool:
        return (
            state_code == StateCode.US_LL
            and raw_data_instance == DirectIngestInstance.PRIMARY
        )

    def test_generates_sql_correctly_with_legacy_raw_data(self) -> None:
        expected_query = """
SELECT file_tag, MAX(update_datetime) AS max_update_datetime
FROM direct_ingest_raw_file_metadata
WHERE raw_data_instance = 'PRIMARY' 
AND is_invalidated = false 
AND file_processed_time IS NOT NULL 
AND region_code = 'US_XX'
GROUP BY file_tag;
"""
        self.assertEqual(
            GetMaxUpdateDateTimeSqlQueryGenerator.legacy_raw_data_sql_query(
                region_code="US_XX", ingest_instance="PRIMARY"
            ),
            expected_query,
        )

    def test_generates_sql_correctly_with_new_raw_data(self) -> None:
        expected_query = """
SELECT file_tag, MAX(update_datetime) AS max_update_datetime
FROM direct_ingest_raw_big_query_file_metadata
WHERE raw_data_instance = 'PRIMARY' 
AND is_invalidated IS FALSE
AND file_processed_time IS NOT NULL 
AND region_code = 'US_XX'
GROUP BY file_tag;
"""
        self.assertEqual(
            GetMaxUpdateDateTimeSqlQueryGenerator.new_raw_data_sql_query(
                region_code="US_XX", ingest_instance="PRIMARY"
            ),
            expected_query,
        )

    @freezegun.freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_max_update_datetimes_retrieved_correctly(self) -> None:
        generator = GetMaxUpdateDateTimeSqlQueryGenerator(region_code="US_XX")

        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)

        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"
        mock_context = {"dag_run": dag_run}

        sample_data = {
            "test_file_tag": "2023-01-26 00:00:00.000000",
        }

        mock_postgres.get_pandas_df.return_value = pd.DataFrame(
            [
                {
                    "file_tag": "test_file_tag",
                    "max_update_datetime": pd.Timestamp(year=2023, month=1, day=26),
                }
            ]
        )

        results = generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        self.assertDictEqual(results, sample_data)

    def test_gating(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)

        us_ll_generator = GetMaxUpdateDateTimeSqlQueryGenerator(region_code="US_LL")
        us_yy_generator = GetMaxUpdateDateTimeSqlQueryGenerator(region_code="US_YY")

        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"
        mock_context = {"dag_run": dag_run}

        us_ll_mock_postgres = create_autospec(PostgresHook)
        us_ll_mock_postgres.get_pandas_df.return_value = pd.DataFrame()

        us_ll_results = us_ll_generator.execute_postgres_query(
            mock_operator, us_ll_mock_postgres, mock_context
        )

        assert us_ll_results == {}

        assert us_ll_mock_postgres.get_pandas_df.call_args[0][
            0
        ] == us_ll_generator.new_raw_data_sql_query(
            StateCode.US_LL.value, DirectIngestInstance.PRIMARY.value
        )

        us_yy_mock_postgres = create_autospec(PostgresHook)
        us_yy_mock_postgres.get_pandas_df.return_value = pd.DataFrame()

        us_yy_results = us_yy_generator.execute_postgres_query(
            mock_operator, us_yy_mock_postgres, mock_context
        )

        assert us_yy_results == {}

        assert us_yy_mock_postgres.get_pandas_df.call_args[0][
            0
        ] == us_yy_generator.legacy_raw_data_sql_query(
            StateCode.US_YY.value, DirectIngestInstance.PRIMARY.value
        )
