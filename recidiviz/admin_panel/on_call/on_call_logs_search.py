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
"""Contains functionality for searching on-call logs in BigQuery"""
import enum

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.utils import metadata

PROCESSED_LOGS_QUERY_TEMPLATE = """
WITH requests_by_status_code AS (
  SELECT
    httpRequest.requestUrl AS url,
    httpRequest.requestMethod AS method,
    httpRequest.status AS status,
    ANY_VALUE(resource.type) AS resource,
    COUNT(*) AS request_count,
    MAX(timestamp) AS latest_response,
    ARRAY_AGG(
      STRUCT (timestamp, trace)
      ORDER BY timestamp DESC
    ) AS traces

  FROM `{project_id}.{on_call_logs_dataset}.{requests_table}`
  WHERE timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY 1, 2, 3
), logs_view AS (
    SELECT 
      url,
      method,
      status,
      request_count, 
      latest_response,
      ANY_VALUE(requests_by_status_code.traces) AS traces,
      (
        SELECT MAX(success_requests.latest_response)
        FROM requests_by_status_code success_requests
        WHERE success_requests.url = requests_by_status_code.url
        AND success_requests.method = requests_by_status_code.method
        AND success_requests.latest_response > requests_by_status_code.latest_response
        AND success_requests.status = 200
      ) AS since_succeeded_timestamp,
      ARRAY_AGG(DISTINCT trace_errors.jsonPayload.message IGNORE NULLS) AS error_logs
    FROM requests_by_status_code
    JOIN UNNEST(requests_by_status_code.traces) AS trace
    LEFT OUTER JOIN `{project_id}.{on_call_logs_dataset}.{traces_table}` trace_errors
      ON trace_errors.trace = trace.trace
      AND trace_errors.severity = "ERROR"
    WHERE requests_by_status_code.status NOT IN  (200, 302, 304, 400, 404, 405)
    AND {view_filter}
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY requests_by_status_code.latest_response DESC
)
SELECT * FROM logs_view WHERE {show_resolved_filter}
"""


class LogsView(enum.Enum):
    DIRECT_INGEST = "direct_ingest"
    APP_ENGINE = "app_engine"
    CLOUD_RUN = "cloud_run"


ON_CALL_LOGS_DATASET = "on_call_logs"


class OnCallLogsSearch:
    """Functionality for searching on-call logs"""

    def __init__(self) -> None:
        self.client: BigQueryClient = BigQueryClientImpl()

    def query(self, view: LogsView, show_resolved: bool = False) -> list[dict]:
        view_filter = 'STARTS_WITH(requests_by_status_code.url, "/direct/")'

        if view == LogsView.APP_ENGINE:
            view_filter = f"NOT {view_filter}"

        if view == LogsView.CLOUD_RUN:
            requests_table = "run_googleapis_com_requests"
            traces_table = "python"
            view_filter = 'requests_by_status_code.resource = "cloud_run_revision"'
        else:
            requests_table = "appengine_googleapis_com_nginx_request"
            traces_table = "app"

        query_builder = BigQueryQueryBuilder(address_overrides=None)
        query = query_builder.build_query(
            project_id=metadata.project_id(),
            query_template=PROCESSED_LOGS_QUERY_TEMPLATE,
            query_format_kwargs={
                "on_call_logs_dataset": ON_CALL_LOGS_DATASET,
                "requests_table": requests_table,
                "traces_table": traces_table,
                "show_resolved_filter": "true"
                if show_resolved
                else "since_succeeded_timestamp is null",
                "view_filter": view_filter,
            },
        )

        return [
            convert_nested_dictionary_keys(row, snake_to_camel)
            for row in self.client.run_query_async(
                query_str=query,
                use_query_cache=False,
            )
        ]
