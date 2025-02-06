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
"""Logic for generating DAG runtime stats"""


from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.source_tables.yaml_managed.datasets import AIRFLOW_OPERATIONS

DAG_RUN_METADATA_TABLE_ADDRESS = BigQueryAddress(
    dataset_id=AIRFLOW_OPERATIONS, table_id="dag_run_metadata"
)
DAG_RUNTIMES_VIEW_ID = "dag_runtimes"
DAG_RUNTIMES_DESCRIPTION = "A view that calculates the end-to-end time of each DAG run."


VIEW_QUERY = """
WITH dag_runs_deduped AS (
  SELECT * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY project_id, dag_id, dag_run_id
                ORDER BY write_time DESC
            ) AS recency_rank
        FROM
            `{project_id}.{airflow_operations_dataset_id}.{dag_run_metadata_table_id}`   
    ) a
    WHERE recency_rank = 1
)
SELECT 
  dag_id, 
  DATE(DATETIME_TRUNC(execution_time, MONTH)) as start_month,
  DATETIME_DIFF(start_time, execution_time, MINUTE) != 0 as has_retried_tasks,
  DATETIME_DIFF(end_time, execution_time, MINUTE) as runtime_in_minutes,
  CASE
    -- "STANDARD" refers to all 'typical' runs; that means all runs for 
    -- the SFTP and monitoring DAGs and state-agnostic PRIMARY runs for raw data and calc
    WHEN 
      JSON_VALUE(dag_run_config,'$.ingest_instance') IS NULL 
    THEN 
      "STANDARD"
    WHEN 
      JSON_VALUE(dag_run_config,'$.ingest_instance') = 'SECONDARY'
    THEN 
      "STATE-SPECIFIC SECONDARY"
    WHEN
      JSON_VALUE(dag_run_config,'$.state_code_filter') is NULL 
    THEN
      "STANDARD"
    ELSE "STATE-SPECIFIC PRIMARY"
  END as run_type
FROM dag_runs_deduped
"""


DAG_RUNTIMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=DAG_RUNTIMES_VIEW_ID,
    description=DAG_RUNTIMES_DESCRIPTION,
    airflow_operations_dataset_id=DAG_RUN_METADATA_TABLE_ADDRESS.dataset_id,
    dag_run_metadata_table_id=DAG_RUN_METADATA_TABLE_ADDRESS.table_id,
    should_materialize=True,
)
