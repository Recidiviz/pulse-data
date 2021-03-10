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
"""Defines subclasses of BigQueryView used in the direct ingest flow.

TODO(#6197): This file should be cleaned up after the new normalized views land.
"""
from typing import List, Optional, Dict

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.views.unnormalized_direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
    RawTableViewType,
    UPDATE_DATETIME_PARAM_NAME,
)
from recidiviz.ingest.direct.query_utils import get_region_raw_file_config


# A parametrized query for looking at the most recent row for each primary key, among rows with update datetimes
# before a certain date.
RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE = f"""
WITH normalized_rows AS (
    SELECT
        {{normalized_columns}}
    FROM
        `{{project_id}}.{{raw_table_dataset_id}}.{{raw_table_name}}`
    WHERE
        update_datetime <= @{UPDATE_DATETIME_PARAM_NAME}
),
rows_with_recency_rank AS (
    SELECT
        {{columns_clause}},
        ROW_NUMBER() OVER (PARTITION BY {{raw_table_primary_key_str}}
                           ORDER BY update_datetime DESC{{supplemental_order_by_clause}}) AS recency_rank
    FROM
        normalized_rows
)

SELECT *
EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""

RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE = f"""
WITH max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `{{project_id}}.{{raw_table_dataset_id}}.{{raw_table_name}}`
    WHERE
        update_datetime <= @{UPDATE_DATETIME_PARAM_NAME}
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `{{project_id}}.{{raw_table_dataset_id}}.{{raw_table_name}}`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
normalized_rows AS (
    SELECT
        {{normalized_columns}}
    FROM
        `{{project_id}}.{{raw_table_dataset_id}}.{{raw_table_name}}`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
),
rows_with_recency_rank AS (
    SELECT
        {{columns_clause}},
        ROW_NUMBER() OVER (PARTITION BY {{raw_table_primary_key_str}}
                           ORDER BY update_datetime DESC{{supplemental_order_by_clause}}) AS recency_rank
    FROM
        normalized_rows
)
SELECT *
EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""


# A query for looking at the most recent row for each primary key
RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE = """
WITH normalized_rows AS (
    SELECT
        {normalized_columns}
    FROM
        `{project_id}.{raw_table_dataset_id}.{raw_table_name}`
),
rows_with_recency_rank AS (
    SELECT
        {columns_clause},
        ROW_NUMBER() OVER (PARTITION BY {raw_table_primary_key_str}
                           ORDER BY update_datetime DESC{supplemental_order_by_clause}) AS recency_rank
    FROM
        normalized_rows
)

SELECT *
EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""

RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE = """
WITH max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `{project_id}.{raw_table_dataset_id}.{raw_table_name}`
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `{project_id}.{raw_table_dataset_id}.{raw_table_name}`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
normalized_rows AS (
    SELECT
        {normalized_columns}
    FROM
        `{project_id}.{raw_table_dataset_id}.{raw_table_name}`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
),
rows_with_recency_rank AS (
    SELECT
        {columns_clause},
        ROW_NUMBER() OVER (PARTITION BY {raw_table_primary_key_str}
                           ORDER BY update_datetime DESC{supplemental_order_by_clause}) AS recency_rank
    FROM
        normalized_rows
)
SELECT *
EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""

DATETIME_COL_NORMALIZATION_TEMPLATE = """
        COALESCE(
            CAST(SAFE_CAST({col_name} AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', {col_name}) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', {col_name}) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', {col_name}) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', {col_name}) AS DATETIME) AS STRING),
            {col_name}
        ) AS {col_name}"""


class NormalizedDirectIngestRawDataTableBigQueryView(BigQueryView):
    """A base class for BigQuery views that give us a view of a region's raw data on a given date."""

    def __init__(
        self,
        *,
        project_id: str = None,
        region_code: str,
        view_id: str,
        view_query_template: str,
        raw_file_config: DirectIngestRawFileConfig,
        dataset_overrides: Optional[Dict[str, str]] = None,
    ):
        if not raw_file_config.primary_key_cols:
            raise ValueError(
                f"Empty primary key list in raw file config with tag [{raw_file_config.file_tag}] during "
                f"construction of DirectIngestRawDataTableBigQueryView"
            )
        view_dataset_id = f"{region_code.lower()}_raw_data_up_to_date_views"
        raw_table_dataset_id = (
            DirectIngestRawFileImportManager.raw_tables_dataset_for_region(region_code)
        )
        columns_clause = self._columns_clause_for_config(raw_file_config)
        supplemental_order_by_clause = self._supplemental_order_by_clause_for_config(
            raw_file_config
        )
        normalized_columns = self._normalized_columns_for_config(raw_file_config)
        super().__init__(
            project_id=project_id,
            dataset_id=view_dataset_id,
            view_id=view_id,
            view_query_template=view_query_template,
            raw_table_dataset_id=raw_table_dataset_id,
            raw_table_name=raw_file_config.file_tag,
            raw_table_primary_key_str=raw_file_config.primary_key_str,
            columns_clause=columns_clause,
            normalized_columns=normalized_columns,
            supplemental_order_by_clause=supplemental_order_by_clause,
            dataset_overrides=dataset_overrides,
        )

    @staticmethod
    def _supplemental_order_by_clause_for_config(
        raw_file_config: DirectIngestRawFileConfig,
    ) -> str:
        if not raw_file_config.supplemental_order_by_clause:
            return ""

        supplemental_order_by_clause = (
            raw_file_config.supplemental_order_by_clause.strip()
        )
        if not supplemental_order_by_clause.startswith(","):
            return ", " + supplemental_order_by_clause

        return supplemental_order_by_clause

    @staticmethod
    def _columns_clause_for_config(raw_file_config: DirectIngestRawFileConfig) -> str:
        columns_str = ", ".join(
            [column.name for column in raw_file_config.columns if column.description]
        )
        return columns_str

    @staticmethod
    def _normalized_columns_for_config(
        raw_file_config: DirectIngestRawFileConfig,
    ) -> str:
        # Right now this only performs normalization for datetime columns, but in the future
        # this method can be expanded to normalize other values.
        if not raw_file_config.datetime_cols:
            return "*"

        datetime_col_str = ", ".join(raw_file_config.datetime_cols)
        return f"* EXCEPT ({datetime_col_str}), " + (
            ", ".join(
                [
                    DATETIME_COL_NORMALIZATION_TEMPLATE.format(col_name=col_name)
                    for col_name in raw_file_config.datetime_cols
                ]
            )
        )


class NormalizedDirectIngestRawDataTableLatestView(
    NormalizedDirectIngestRawDataTableBigQueryView
):
    """A BigQuery view with a query for the given |raw_table_name|, which when used will load the most up-to-date values
    of all rows in that table.
    """

    def __init__(
        self,
        *,
        project_id: str = None,
        region_code: str,
        raw_file_config: DirectIngestRawFileConfig,
        dataset_overrides: Optional[Dict[str, str]],
    ):
        view_id = f"{raw_file_config.file_tag}_latest"
        view_query_template = (
            RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE
            if raw_file_config.always_historical_export
            else RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE
        )
        super().__init__(
            project_id=project_id,
            region_code=region_code,
            view_id=view_id,
            view_query_template=view_query_template,
            raw_file_config=raw_file_config,
            dataset_overrides=dataset_overrides,
        )


# NOTE: BigQuery does not support parametrized queries for views, so we can't actually upload this as a view until this
# issue is resolved: https://issuetracker.google.com/issues/35905221. For now, we construct it like a BigQueryView, but
# just use the view_query field to get a query we can execute to pull data in direct ingest.
class NormalizedDirectIngestRawDataTableUpToDateView(
    NormalizedDirectIngestRawDataTableBigQueryView
):
    """A view with a parametrized query for the given |raw_file_config|. The caller is responsible for filling out
    the parameter. When used, this query will load all rows in the provided table up to the date of the provided date
    parameter.
    """

    def __init__(
        self,
        *,
        project_id: str = None,
        region_code: str,
        raw_file_config: DirectIngestRawFileConfig,
    ):
        view_id = f"{raw_file_config.file_tag}_by_update_date"
        view_query_template = (
            RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE
            if raw_file_config.always_historical_export
            else RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE
        )
        super().__init__(
            project_id=project_id,
            region_code=region_code,
            view_id=view_id,
            view_query_template=view_query_template,
            raw_file_config=raw_file_config,
        )


class NormalizedDirectIngestPreProcessedIngestView(DirectIngestPreProcessedIngestView):
    @staticmethod
    def _date_bounded_query_for_raw_table(
        region_code: str,
        raw_table_config: DirectIngestRawFileConfig,
        parametrize_query: bool,
    ) -> str:
        if parametrize_query:
            return NormalizedDirectIngestRawDataTableUpToDateView(
                region_code=region_code, raw_file_config=raw_table_config
            ).view_query
        return NormalizedDirectIngestRawDataTableLatestView(
            region_code=region_code,
            raw_file_config=raw_table_config,
            dataset_overrides=None,
        ).select_query_uninjected_project_id


class NormalizedDirectIngestPreProcessedIngestViewBuilder(
    BigQueryViewBuilder[NormalizedDirectIngestPreProcessedIngestView]
):
    """Factory class for building NormalizedDirectIngestPreProcessedIngestView"""

    def __init__(
        self,
        *,
        region: str,
        ingest_view_name: str,
        view_query_template: str,
        order_by_cols: Optional[str],
        is_detect_row_deletion_view: bool = False,
        primary_key_tables_for_entity_deletion: Optional[List[str]] = None,
        materialize_raw_data_table_views: bool = False,
    ):
        self.region = region
        self.ingest_view_name = ingest_view_name
        self.view_query_template = view_query_template
        self.order_by_cols = order_by_cols
        self.is_detect_row_deletion_view = is_detect_row_deletion_view
        self.primary_key_tables_for_entity_deletion = (
            primary_key_tables_for_entity_deletion or []
        )
        self.materialize_raw_data_table_views = materialize_raw_data_table_views

    @property
    def file_tag(self) -> str:
        return self.ingest_view_name

    # pylint: disable=unused-argument
    def _build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> NormalizedDirectIngestPreProcessedIngestView:
        """Builds an instance of a NormalizedDirectIngestPreProcessedIngestView with the provided args."""
        return NormalizedDirectIngestPreProcessedIngestView(
            ingest_view_name=self.ingest_view_name,
            view_query_template=self.view_query_template,
            region_raw_table_config=get_region_raw_file_config(self.region),
            order_by_cols=self.order_by_cols,
            is_detect_row_deletion_view=self.is_detect_row_deletion_view,
            primary_key_tables_for_entity_deletion=self.primary_key_tables_for_entity_deletion,
            materialize_raw_data_table_views=self.materialize_raw_data_table_views,
        )

    def build_and_print(self) -> None:
        """For local testing, prints out the parametrized and latest versions of the view's query."""
        view = self.build()
        print(
            "****************************** PARAMETRIZED ******************************"
        )
        print(
            view.expanded_view_query(
                config=NormalizedDirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.PARAMETERIZED,
                )
            )
        )
        print(
            "********************************* LATEST *********************************"
        )
        print(
            view.expanded_view_query(
                config=NormalizedDirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.LATEST,
                )
            )
        )

    def should_build(self) -> bool:
        return True
