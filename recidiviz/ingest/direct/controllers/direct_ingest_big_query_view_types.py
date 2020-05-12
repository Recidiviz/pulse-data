# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Defines subclasses of BigQueryView used in the direct ingest flow."""
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRawFileConfig, \
    DirectIngestRawFileImportManager

UPDATE_DATETIME_PARAM_NAME = "update_timestamp"

# A parametrized query for looking at the most recent row for each primary key, among rows with update datetimes
# before a certain date.
RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE = f"""
WITH rows_with_recency_rank AS (
   SELECT 
      *, 
      ROW_NUMBER() OVER (PARTITION BY {{raw_table_primary_key_str}} ORDER BY update_datetime DESC) AS recency_rank
   FROM 
      `{{project_id}}.{{raw_table_dataset_id}}.{{raw_table_name}}`
   WHERE 
       update_datetime <= @{UPDATE_DATETIME_PARAM_NAME}
)

SELECT * 
EXCEPT (file_id, recency_rank, update_datetime)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""

# A query for looking at the most recent row for each primary key
RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE = """
WITH rows_with_recency_rank AS (
   SELECT 
      *, 
      ROW_NUMBER() OVER (PARTITION BY {raw_table_primary_key_str} ORDER BY update_datetime DESC) AS recency_rank
   FROM 
      `{project_id}.{raw_table_dataset_id}.{raw_table_name}`
)

SELECT * 
EXCEPT (file_id, recency_rank, update_datetime)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""


class DirectIngestRawDataTableBigQueryView(BigQueryView):
    """A base class for BigQuery views that give us a view of a region's raw data on a given date."""
    def __init__(self, *,
                 project_id: str = None,
                 region_code: str,
                 view_id: str,
                 view_query_template: str,
                 raw_file_config: DirectIngestRawFileConfig):
        view_dataset_id = f'{region_code.lower()}_raw_data_up_to_date_views'
        raw_table_dataset_id = DirectIngestRawFileImportManager.raw_tables_dataset_for_region(region_code)
        super().__init__(project_id=project_id,
                         dataset_id=view_dataset_id,
                         view_id=view_id,
                         view_query_template=view_query_template,
                         raw_table_dataset_id=raw_table_dataset_id,
                         raw_table_name=raw_file_config.file_tag,
                         raw_table_primary_key_str=raw_file_config.primary_key_str)


class DirectIngestRawDataTableLatestBigQueryView(DirectIngestRawDataTableBigQueryView):
    """A BigQuery view with a query for the given |raw_table_name|, which when used will load the most up-to-date values
    of all rows in that table.
    """
    def __init__(self,
                 *,
                 project_id: str = None,
                 region_code: str,
                 raw_file_config: DirectIngestRawFileConfig):
        view_id = f'{raw_file_config.file_tag}_latest'
        super().__init__(project_id=project_id,
                         region_code=region_code,
                         view_id=view_id,
                         view_query_template=RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE,
                         raw_file_config=raw_file_config)


# NOTE: BigQuery does not support parametrized queries for views, so we can't actually upload this as a view until this
# issue is resolved: https://issuetracker.google.com/issues/35905221. For now, we construct it like a BigQueryView, but
# just use the view_query field to get a query we can execute to pull data in direct ingest.
class DirectIngestRawDataTableUpToDateBigQueryView(DirectIngestRawDataTableBigQueryView):
    """A view with a parametrized query for the given |raw_file_config|. The caller is responsible for filling out
    the parameter. When used, this query will load all rows in the provided table up to the date of the provided date
    parameter.
    """
    def __init__(self,
                 *,
                 project_id: str = None,
                 region_code: str,
                 raw_file_config: DirectIngestRawFileConfig):
        view_id = f'{raw_file_config.file_tag}_by_update_date'
        super().__init__(project_id=project_id,
                         region_code=region_code,
                         view_id=view_id,
                         view_query_template=RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE,
                         raw_file_config=raw_file_config)
