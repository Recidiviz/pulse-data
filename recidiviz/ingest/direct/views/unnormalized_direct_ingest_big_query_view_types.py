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
import re
import string
from enum import Enum, auto
from typing import List, Optional, Dict

import attr

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileImportManager,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.query_utils import get_region_raw_file_config

UPDATE_DATETIME_PARAM_NAME = "update_timestamp"

# A parametrized query for looking at the most recent row for each primary key, among rows with update datetimes
# before a certain date.
RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE = f"""
WITH rows_with_recency_rank AS (
    SELECT
        {{columns_clause}},
        ROW_NUMBER() OVER (PARTITION BY {{raw_table_primary_key_str}}
                           ORDER BY update_datetime DESC{{supplemental_order_by_clause}}) AS recency_rank
    FROM
        `{{project_id}}.{{raw_table_dataset_id}}.{{raw_table_name}}`
    WHERE
        update_datetime <= @{UPDATE_DATETIME_PARAM_NAME}
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
rows_with_recency_rank AS (
    SELECT
        {{columns_clause}},
        ROW_NUMBER() OVER (PARTITION BY {{raw_table_primary_key_str}}
                           ORDER BY update_datetime DESC{{supplemental_order_by_clause}}) AS recency_rank
    FROM
        `{{project_id}}.{{raw_table_dataset_id}}.{{raw_table_name}}`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)
SELECT *
EXCEPT (recency_rank)
FROM rows_with_recency_rank
WHERE recency_rank = 1
"""


# A query for looking at the most recent row for each primary key
RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE = """
WITH rows_with_recency_rank AS (
    SELECT 
        {columns_clause},
        ROW_NUMBER() OVER (PARTITION BY {raw_table_primary_key_str}
                           ORDER BY update_datetime DESC{supplemental_order_by_clause}) AS recency_rank
    FROM 
        `{project_id}.{raw_table_dataset_id}.{raw_table_name}`
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
rows_with_recency_rank AS (
    SELECT 
        {columns_clause},
        ROW_NUMBER() OVER (PARTITION BY {raw_table_primary_key_str}
                           ORDER BY update_datetime DESC{supplemental_order_by_clause}) AS recency_rank
    FROM 
        `{project_id}.{raw_table_dataset_id}.{raw_table_name}`
    WHERE 
        file_id = (SELECT file_id FROM max_file_id)
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

CREATE_TEMP_TABLE_REGEX = re.compile(r"CREATE\s+((TEMP|TEMPORARY)\s+)TABLE")


DESTINATION_TABLE_QUERY_FORMAT = """{raw_materialized_tables_clause}
DROP TABLE IF EXISTS `{{project_id}}.{dataset_id}.{table_id}`;
CREATE TABLE `{{project_id}}.{dataset_id}.{table_id}`
OPTIONS(
  -- Data in this table will be deleted after 24 hours
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (

{select_query_clause}

);
"""

DESTINATION_TEMP_TABLE_QUERY_FORMAT = """{raw_materialized_tables_clause}
CREATE TEMP TABLE {table_id} AS (

{select_query_clause}

);
"""


class UnnormalizedDirectIngestRawDataTableBigQueryView(BigQueryView):
    """A base class for BigQuery views that give us a view of a region's raw data on a given date."""

    def __init__(
        self,
        *,
        project_id: str = None,
        region_code: str,
        description: str,
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
        super().__init__(
            project_id=project_id,
            dataset_id=view_dataset_id,
            view_id=view_id,
            description=description,
            view_query_template=view_query_template,
            raw_table_dataset_id=raw_table_dataset_id,
            raw_table_name=raw_file_config.file_tag,
            raw_table_primary_key_str=raw_file_config.primary_key_str,
            columns_clause=columns_clause,
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
            [
                column.name
                if not column.is_datetime and column.description
                else DATETIME_COL_NORMALIZATION_TEMPLATE.format(col_name=column.name)
                for column in raw_file_config.columns
                if column.description
            ]
        )
        return f"{columns_str}"


class UnnormalizedDirectIngestRawDataTableLatestView(
    UnnormalizedDirectIngestRawDataTableBigQueryView
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
        description = f"{raw_file_config.file_tag} latest view"
        view_query_template = (
            RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE
            if raw_file_config.always_historical_export
            else RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE
        )
        super().__init__(
            project_id=project_id,
            region_code=region_code,
            view_id=view_id,
            description=description,
            view_query_template=view_query_template,
            raw_file_config=raw_file_config,
            dataset_overrides=dataset_overrides,
        )


# NOTE: BigQuery does not support parametrized queries for views, so we can't actually upload this as a view until this
# issue is resolved: https://issuetracker.google.com/issues/35905221. For now, we construct it like a BigQueryView, but
# just use the view_query field to get a query we can execute to pull data in direct ingest.
class UnnormalizedDirectIngestRawDataTableUpToDateView(
    UnnormalizedDirectIngestRawDataTableBigQueryView
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
        description = f"{raw_file_config.file_tag} parametrized view"
        view_query_template = (
            RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE
            if raw_file_config.always_historical_export
            else RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE
        )
        super().__init__(
            project_id=project_id,
            region_code=region_code,
            view_id=view_id,
            description=description,
            view_query_template=view_query_template,
            raw_file_config=raw_file_config,
        )


class RawTableViewType(Enum):
    # Raw table view subqueries will take form of non-parametrized queries that return the latest version of each raw
    # table. This type will produce a query that can run in the BigQuery UI and that can be used easily for debugging.
    LATEST = auto()

    # Raw table views will take the form of parameterized queries that return the the version of the raw table up to the
    # a max update date parameter. By default, this parameter name will be UPDATE_DATETIME_PARAM_NAME.
    PARAMETERIZED = auto()


class DestinationTableType(Enum):
    # The query will be structured to end with a SELECT statement. This can be used for queries that may be run in the
    # BigQuery UI.
    NONE = auto()

    # The query will be structured to write the results of the SELECT statement to a temporary table in the script (via
    # a CREATE TEMP TABLE statement). Should be used if the results of this view query will be used as part of a larger
    # script.
    TEMPORARY = auto()

    # The query will be structured to write the results of the SELECT statement to an output table with a 24 hour
    # expiration (via a CREATE TABLE statement). Initialization will throw if this is set without nonnull
    # destination_table_id and destination_dataset_id.
    PERMANENT_EXPIRING = auto()


class DirectIngestPreProcessedIngestView(BigQueryView):
    """Class for holding direct ingest pre-processing SQL queries, that can be used to export files for import into our
    Postgres DB.
    """

    @attr.s
    class QueryStructureConfig:
        """Configuration for how to structure the expanded view query with hydrated raw table views."""

        # Specifies the structure of the raw table view subqueries
        raw_table_view_type: RawTableViewType = attr.ib()

        # For queries with a |raw_table_view_type| of PARAMETRIZED, this may be to set to specify a custom name for the
        # max update date parameter. If it is not set, the parameter name will be the default
        # UPDATE_DATETIME_PARAM_NAME.
        param_name_override: Optional[str] = attr.ib(default=None)

        # Specifies whether the query should be structured to write results to a destination table and the type of that
        # table.
        destination_table_type: DestinationTableType = attr.ib(
            default=DestinationTableType.NONE
        )

        # The destination dataset id for queries with destination_table_type PERMANENT_EXPIRING.
        destination_dataset_id: Optional[str] = attr.ib(default=None)

        # The destination table id for queries with destination_table_types other than NONE.
        destination_table_id: Optional[str] = attr.ib(default=None)

        # Prefix to apply to all raw table subquery names (or temp table names if raw table subqueries are
        # materialized). Can be used when multiple temp tables for the same raw table are created in the same script
        # (e.g. two that each have different date bounds).
        raw_table_subquery_name_prefix: Optional[str] = attr.ib(default=None)

        @property
        def parameterize_raw_data_table_views(self) -> bool:
            return self.raw_table_view_type == RawTableViewType.PARAMETERIZED

        def __attrs_post_init__(self) -> None:
            if not self.parameterize_raw_data_table_views and self.param_name_override:
                raise ValueError(
                    f"Found nonnull param_name_override [{self.param_name_override}] which should only be "
                    f"set if parameterize_raw_data_table_views is True."
                )

            if (
                self.destination_dataset_id
                and self.destination_table_type
                != DestinationTableType.PERMANENT_EXPIRING
            ):
                raise ValueError(
                    f"Found nonnull destination_dataset_id [{self.destination_dataset_id}] with "
                    f"destination_table_type [{self.destination_table_type.name}]"
                )

            if (
                not self.destination_dataset_id
                and self.destination_table_type
                == DestinationTableType.PERMANENT_EXPIRING
            ):
                raise ValueError(
                    f"Found null destination_dataset_id [{self.destination_dataset_id}] with "
                    f"destination_table_type [{self.destination_table_type.name}]"
                )

            if (
                self.destination_table_id
                and self.destination_table_type == DestinationTableType.NONE
            ):
                raise ValueError(
                    f"Found nonnull destination_table_id [{self.destination_table_id}] with "
                    f"destination_table_type [{self.destination_table_type.name}]"
                )

            if (
                not self.destination_table_id
                and self.destination_table_type != DestinationTableType.NONE
            ):
                raise ValueError(
                    f"Found null destination_table_id [{self.destination_table_id}] with "
                    f"destination_table_type [{self.destination_table_type.name}]"
                )

    WITH_PREFIX = "WITH"
    SUBQUERY_INDENT = "    "

    def __init__(
        self,
        *,
        ingest_view_name: str,
        view_query_template: str,
        region_raw_table_config: DirectIngestRegionRawFileConfig,
        order_by_cols: Optional[str],
        is_detect_row_deletion_view: bool,
        primary_key_tables_for_entity_deletion: List[str],
        materialize_raw_data_table_views: bool = False,
    ):
        """Builds a view for holding direct ingest pre-processing SQL queries, that can be used to export files for
        import into our Postgres DB.

        Args:
            ingest_view_name: (str) The name of the view.
            view_query_template: (str) The template for the query, formatted for hydration of raw table views.
            region_raw_table_config: (DirectIngestRegionRawFileConfig) Raw table configurations for the region this
                view corresponds to.
            order_by_cols: (str) An optional, comma-separated string of columns to sort the final results by.
            is_detect_row_deletion_view: (bool) When true, this view will be built to detect that rows have been deleted
                since the previous raw data update for this view.
            primary_key_tables_for_entity_deletion: (str) A list of table names that are used to build the primary keys
                for this table - must be non-empty when |is_detect_row_deletion_view| is True.
            materialize_raw_data_table_views: (bool) When True, the raw table subqueries for this query will be hydrated
                as separate, materialized CREATE TEMP TABLE statements. Should be used for queries that are too complex
                to run otherwise (i.e. they produce a 'too many subqueries' error). This will slow down your query by a
                factor of 4-5.
                IMPORTANT NOTE: When this is True, the view query will become a "script" which means it cannot be used
                in the # Python BigQuery API for a query that sets a destination table
                (bigquery.QueryJobConfig#destination is not None).
        """
        DirectIngestPreProcessedIngestView._validate_order_by(
            ingest_view_name=ingest_view_name, view_query_template=view_query_template
        )

        self._region_code = region_raw_table_config.region_code
        self._raw_table_dependency_configs = self._get_raw_table_dependency_configs(
            view_query_template, region_raw_table_config
        )

        latest_view_query = self._format_expanded_view_query(
            region_code=self._region_code,
            raw_table_dependency_configs=self._raw_table_dependency_configs,
            view_query_template=view_query_template,
            order_by_cols=order_by_cols,
            materialize_raw_data_table_views=materialize_raw_data_table_views,
            config=DirectIngestPreProcessedIngestView.QueryStructureConfig(
                raw_table_view_type=RawTableViewType.LATEST
            ),
        )

        dataset_id = f"{self._region_code.lower()}_ingest_views"
        description = f"{ingest_view_name} ingest view"
        super().__init__(
            dataset_id=dataset_id,
            view_id=ingest_view_name,
            description=description,
            view_query_template=latest_view_query,
        )

        self._view_query_template = view_query_template
        self._order_by_cols = order_by_cols
        self._is_detect_row_deletion_view = is_detect_row_deletion_view
        self._materialize_raw_data_table_views = materialize_raw_data_table_views
        if self._is_detect_row_deletion_view:
            self._validate_can_detect_row_deletion(
                raw_configs=self._raw_table_dependency_configs,
                ingest_view_name=ingest_view_name,
                primary_key_tables_for_entity_deletion=primary_key_tables_for_entity_deletion,
            )

        if re.search(CREATE_TEMP_TABLE_REGEX, view_query_template):
            raise ValueError(
                "Found CREATE TEMP TABLE clause in this query - ingest views cannot contain CREATE clauses."
            )

    @property
    def file_tag(self) -> str:
        """The file tag that should be written to any file export of this query."""
        return self.view_id

    @property
    def raw_table_dependency_configs(self) -> List[DirectIngestRawFileConfig]:
        """Configs for any raw tables that this view's query depends on."""
        return self._raw_table_dependency_configs

    @property
    def do_reverse_date_diff(self) -> bool:
        """True if this view represents a date diff query that must provide the contents from DATE 1 that are not
        present at, or have changed since, DATE 2. If False does the traditional date diff by looking at contents from
        DATE 2 that are not present at, or have changed since, DATE 1.
        """
        # Do reverse date diff if detecting rows deleted from this ingest view between exports.
        return self.is_detect_row_deletion_view

    @property
    def is_detect_row_deletion_view(self) -> bool:
        """True only if this view should be used to generate rows that are deleted from this ingest view between state
        exports. This can only be true if we receive full historical exports for all raw data files that are
        responsible for creating new rows in the ingest view and this query only produces id columns that show the
        presence or absence of a row.
        """
        return self._is_detect_row_deletion_view

    @property
    def order_by_cols(self) -> Optional[str]:
        """String containing any columns used to order the ingest view query results. This string will be appended to
        ingest view queries in the format `ORDER BY {order_by_cols}, and therefore |order_by_cols| must create valid
        SQL when appended in that fashion.

        Examples values:
            "col1, col2"
            "col1 ASC, col2 DESC"
            "CAST(col1) AS INT64, col2"
        """
        return self._order_by_cols

    @property
    def materialize_raw_data_table_views(self) -> bool:
        """If True, this query will always materialize raw table views into temporary tables."""
        return self._materialize_raw_data_table_views

    def expanded_view_query(
        self, config: "DirectIngestPreProcessedIngestView.QueryStructureConfig"
    ) -> str:
        """Formats this view's template according to the provided config, with expanded subqueries for each raw table
        dependency."""
        query = self._format_expanded_view_query(
            region_code=self._region_code,
            raw_table_dependency_configs=self._raw_table_dependency_configs,
            view_query_template=self._view_query_template,
            order_by_cols=self._order_by_cols,
            materialize_raw_data_table_views=self._materialize_raw_data_table_views,
            config=config,
        )
        return query.format(**self._query_format_args_with_project_id())

    @staticmethod
    def _table_subbquery_name(
        raw_table_config: DirectIngestRawFileConfig, prefix: Optional[str]
    ) -> str:
        """The name for the expanded subquery on this raw table."""

        prefix = prefix or ""
        return f"{prefix}{raw_table_config.file_tag}_generated_view"

    @staticmethod
    def add_order_by_suffix(query: str, order_by_cols: Optional[str]) -> str:
        if order_by_cols:
            query = query.rstrip().rstrip(";")
            query = f"{query}\nORDER BY {order_by_cols};"
        return query

    @classmethod
    def _raw_table_subquery_clause(
        cls,
        *,
        region_code: str,
        raw_table_dependency_configs: List[DirectIngestRawFileConfig],
        parametrize_query: bool,
        raw_table_subquery_name_prefix: Optional[str],
        materialize_raw_data_table_views: bool,
    ) -> str:
        """Returns the portion of the script that generates the raw table view queries, either as a list of
        `CREATE TEMP TABLE` statements or a list of WITH subqueries.
        """
        table_subquery_strs = []
        for raw_table_config in raw_table_dependency_configs:
            table_subquery_strs.append(
                cls._get_table_subquery_str(
                    region_code,
                    raw_table_config,
                    parametrize_query,
                    raw_table_subquery_name_prefix,
                )
            )

        if materialize_raw_data_table_views:
            temp_table_query_strs = [
                f"CREATE TEMP TABLE {table_subquery_str};"
                for table_subquery_str in table_subquery_strs
            ]
            table_subquery_clause = "\n".join(temp_table_query_strs)
            return f"{table_subquery_clause}"

        table_subquery_clause = ",\n".join(table_subquery_strs)
        return f"{cls.WITH_PREFIX}\n{table_subquery_clause}"

    @classmethod
    def _get_raw_materialized_tables_clause(
        cls,
        region_code: str,
        raw_table_dependency_configs: List[DirectIngestRawFileConfig],
        materialize_raw_data_table_views: bool,
        config: "DirectIngestPreProcessedIngestView.QueryStructureConfig",
    ) -> Optional[str]:
        """Returns the clause with all the temporary raw data tables that must be created before the final SELECT/CREATE
        statement.
        """

        if not materialize_raw_data_table_views:
            return None

        return cls._raw_table_subquery_clause(
            region_code=region_code,
            raw_table_dependency_configs=raw_table_dependency_configs,
            parametrize_query=config.parameterize_raw_data_table_views,
            raw_table_subquery_name_prefix=config.raw_table_subquery_name_prefix,
            materialize_raw_data_table_views=materialize_raw_data_table_views,
        )

    @classmethod
    def _get_select_query_clause(
        cls,
        region_code: str,
        raw_table_dependency_configs: List[DirectIngestRawFileConfig],
        view_query_template: str,
        order_by_cols: Optional[str],
        materialize_raw_data_table_views: bool,
        config: "DirectIngestPreProcessedIngestView.QueryStructureConfig",
    ) -> str:
        """Returns the final SELECT statement that produces the results for this ingest view query. It will either
        pull in raw table data as WITH subqueries or reference materialized temporary tables with raw table data.
        """
        view_query_template = view_query_template.strip()
        if materialize_raw_data_table_views:
            # The template references raw table views that will be prepended to the query script.
            select_query_clause = view_query_template

        else:
            raw_table_subquery_clause = cls._raw_table_subquery_clause(
                region_code=region_code,
                raw_table_dependency_configs=raw_table_dependency_configs,
                parametrize_query=config.parameterize_raw_data_table_views,
                raw_table_subquery_name_prefix=config.raw_table_subquery_name_prefix,
                materialize_raw_data_table_views=materialize_raw_data_table_views,
            )

            if view_query_template.startswith(cls.WITH_PREFIX):
                view_query_template = view_query_template[
                    len(cls.WITH_PREFIX) :
                ].lstrip()
                raw_table_subquery_clause = raw_table_subquery_clause + ","

            select_query_clause = f"{raw_table_subquery_clause}\n{view_query_template}"
        select_query_clause = cls.add_order_by_suffix(
            query=select_query_clause, order_by_cols=order_by_cols
        )
        select_query_clause = select_query_clause.rstrip().rstrip(";")
        return select_query_clause

    @classmethod
    def _get_full_query_template(
        cls,
        region_code: str,
        raw_table_dependency_configs: List[DirectIngestRawFileConfig],
        view_query_template: str,
        order_by_cols: Optional[str],
        materialize_raw_data_table_views: bool,
        config: "DirectIngestPreProcessedIngestView.QueryStructureConfig",
    ) -> str:
        """Returns the full, formatted ingest view query template that can be injected with format args."""
        raw_materialized_tables_clause = (
            cls._get_raw_materialized_tables_clause(
                region_code=region_code,
                raw_table_dependency_configs=raw_table_dependency_configs,
                materialize_raw_data_table_views=materialize_raw_data_table_views,
                config=config,
            )
            or ""
        )

        select_query_clause = cls._get_select_query_clause(
            region_code=region_code,
            raw_table_dependency_configs=raw_table_dependency_configs,
            view_query_template=view_query_template,
            order_by_cols=order_by_cols,
            materialize_raw_data_table_views=materialize_raw_data_table_views,
            config=config,
        )

        if config.destination_table_type == DestinationTableType.PERMANENT_EXPIRING:
            return DESTINATION_TABLE_QUERY_FORMAT.format(
                raw_materialized_tables_clause=raw_materialized_tables_clause,
                dataset_id=config.destination_dataset_id,
                table_id=config.destination_table_id,
                select_query_clause=select_query_clause,
            )
        if config.destination_table_type == DestinationTableType.TEMPORARY:
            return DESTINATION_TEMP_TABLE_QUERY_FORMAT.format(
                raw_materialized_tables_clause=raw_materialized_tables_clause,
                table_id=config.destination_table_id,
                select_query_clause=select_query_clause,
            )
        if config.destination_table_type == DestinationTableType.NONE:
            return f"{raw_materialized_tables_clause}\n{select_query_clause};"

        raise ValueError(
            f"Unsupported destination_table_type: [{config.destination_table_type.name}]"
        )

    @classmethod
    def _format_expanded_view_query(
        cls,
        region_code: str,
        raw_table_dependency_configs: List[DirectIngestRawFileConfig],
        view_query_template: str,
        order_by_cols: Optional[str],
        materialize_raw_data_table_views: bool,
        config: "DirectIngestPreProcessedIngestView.QueryStructureConfig",
    ) -> str:
        """Formats the given template with expanded subqueries for each raw table dependency according to the given
        config. Does not hydrate the project_id so the result of this function can be passed as a template to the
        superclass constructor.
        """
        full_query_template = cls._get_full_query_template(
            region_code=region_code,
            raw_table_dependency_configs=raw_table_dependency_configs,
            view_query_template=view_query_template,
            order_by_cols=order_by_cols,
            materialize_raw_data_table_views=materialize_raw_data_table_views,
            config=config,
        )

        format_args = {}
        for raw_table_config in raw_table_dependency_configs:
            format_args[raw_table_config.file_tag] = cls._table_subbquery_name(
                raw_table_config, config.raw_table_subquery_name_prefix
            )

        # We don't want to inject the project_id outside of the BigQueryView initializer
        query = cls._format_view_query_without_project_id(
            full_query_template, **format_args
        )

        if config.param_name_override:
            query = query.replace(
                f"@{UPDATE_DATETIME_PARAM_NAME}", f"@{config.param_name_override}"
            )

        return query.strip()

    @classmethod
    def _get_table_subquery_str(
        cls,
        region_code: str,
        raw_table_config: DirectIngestRawFileConfig,
        parametrize_query: bool,
        raw_table_subquery_name_prefix: Optional[str],
    ) -> str:
        """Returns an expanded subquery on this raw table in the form 'subquery_name AS (...)'."""
        date_bounded_query = cls._date_bounded_query_for_raw_table(
            region_code=region_code,
            raw_table_config=raw_table_config,
            parametrize_query=parametrize_query,
        )
        date_bounded_query = date_bounded_query.strip("\n")
        indented_date_bounded_query = cls.SUBQUERY_INDENT + date_bounded_query.replace(
            "\n", "\n" + cls.SUBQUERY_INDENT
        )

        indented_date_bounded_query = indented_date_bounded_query.replace(
            f"\n{cls.SUBQUERY_INDENT}\n", "\n\n"
        )
        table_subquery_name = cls._table_subbquery_name(
            raw_table_config, raw_table_subquery_name_prefix
        )
        return f"{table_subquery_name} AS (\n{indented_date_bounded_query}\n)"

    @classmethod
    def _get_raw_table_dependency_configs(
        cls,
        view_query_template: str,
        region_raw_table_config: DirectIngestRegionRawFileConfig,
    ) -> List[DirectIngestRawFileConfig]:
        """Returns a sorted list of configs for all raw files this query depends on."""
        raw_table_dependencies = cls._parse_raw_table_dependencies(view_query_template)
        raw_table_dependency_configs = []
        for raw_table_tag in raw_table_dependencies:
            if raw_table_tag not in region_raw_table_config.raw_file_configs:
                raise ValueError(f"Found unexpected raw table tag [{raw_table_tag}]")
            if not region_raw_table_config.raw_file_configs[raw_table_tag].columns:
                raise ValueError(
                    f"Found empty set of columns in raw table config [{raw_table_tag}]"
                    f" in region [{region_raw_table_config.region_code}]."
                )
            if not region_raw_table_config.raw_file_configs[
                raw_table_tag
            ].primary_key_cols:
                raise ValueError(
                    f"Empty primary key list in raw file config with tag [{raw_table_tag}]"
                )
            raw_table_dependency_configs.append(
                region_raw_table_config.raw_file_configs[raw_table_tag]
            )
        return raw_table_dependency_configs

    @staticmethod
    def _parse_raw_table_dependencies(view_query_template: str) -> List[str]:
        """Parses and returns all format args in the view query template (should be only raw table names) and returns as
        a sorted list."""
        dependencies_set = {
            field_name
            for _, field_name, _, _ in string.Formatter().parse(view_query_template)
            if field_name is not None
        }
        return sorted(dependencies_set)

    @staticmethod
    def _date_bounded_query_for_raw_table(
        region_code: str,
        raw_table_config: DirectIngestRawFileConfig,
        parametrize_query: bool,
    ) -> str:
        """Should be implemented to look like:
        ```
        if parametrize_query:
            return DirectIngestRawDataTableUpToDateView(
                region_code=region_code, raw_file_config=raw_table_config
            ).view_query
        return DirectIngestRawDataTableLatestView(
            region_code=region_code,
            raw_file_config=raw_table_config,
            dataset_overrides=None,
        ).select_query_uninjected_project_id
        ```
        for specific DirectIngestRawDataTable* classes.
        """
        raise NotImplementedError(
            "this should be impelmented in subclasses until TODO(#6197) cleans this up"
        )

    @staticmethod
    def _validate_order_by(ingest_view_name: str, view_query_template: str) -> None:
        query = view_query_template.upper()
        final_sub_query = query.split("FROM")[-1]
        order_by_count = final_sub_query.count("ORDER BY")
        if order_by_count:
            raise ValueError(
                f"Found ORDER BY after the final FROM statement in the SQL view_query_template for "
                f"{ingest_view_name}. Please ensure that all ordering of the final query is done by specifying "
                f"DirectIngestPreProcessedIngestView.order_by_cols instead of putting an ORDER BY "
                f"clause in DirectIngestPreProcessingIngestView.view_query_template. If this ORDER BY is a result"
                f"of an inline subquery in the final SELECT statement, please consider moving alias-ing the subquery "
                f"or otherwise refactoring the query so no ORDER BY statements occur after the final `FROM`"
            )

    @staticmethod
    def _validate_can_detect_row_deletion(
        ingest_view_name: str,
        primary_key_tables_for_entity_deletion: List[str],
        raw_configs: List[DirectIngestRawFileConfig],
    ) -> None:
        if not primary_key_tables_for_entity_deletion:
            raise ValueError(
                f"Ingest view {ingest_view_name} was marked as `is_detect_row_deletion_view`; however no "
                f"`primary_key_tables_for_entity_deletion` were defined. When the view is constructed, "
                f"please specify all raw tables necessary for generating the primary key of the to-be-"
                f"deleted entity into this ingest field."
            )

        raw_config_file_tags = [r.file_tag for r in raw_configs]
        for primary_key_table_name in primary_key_tables_for_entity_deletion:
            if primary_key_table_name not in raw_config_file_tags:
                raise ValueError(
                    f"Ingest view {ingest_view_name} has specified {primary_key_table_name} in "
                    f"`primary_key_tables_for_entity_deletion`, but that raw file tag was not found as a dependency. "
                    f"Please make sure all tables specified in `primary_key_tables_for_entity_deletion` appear in the "
                    f"ingest view's query."
                )

        for raw_config in raw_configs:
            if raw_config.file_tag not in primary_key_tables_for_entity_deletion:
                continue

            if not raw_config.always_historical_export:
                raise ValueError(
                    f"Ingest view {ingest_view_name} is marked as `is_detect_row_deletion_view` and has table "
                    f"{raw_config.file_tag} specified in `primary_key_tables_for_entity_deletion`; however the raw "
                    f"data file is not marked as always being exported as historically. For "
                    f"`is_detect_row_deletion_view` to be True, we must receive historical exports for all tables "
                    f"that provide the primary keys of the entity to be deleted. Please ensure that "
                    f"`primary_key_tables_for_entity_deletion`, and the raw table configs for those specified tables "
                    f"are up to date. If this is up to date, and we don't receive historical exports for one of the "
                    f"tables responsible for generating the to be deleted entity's primary key, then we cannot "
                    f"do row deletion detection."
                )


class UnnormalizedDirectIngestPreProcessedIngestView(
    DirectIngestPreProcessedIngestView
):
    @staticmethod
    def _date_bounded_query_for_raw_table(
        region_code: str,
        raw_table_config: DirectIngestRawFileConfig,
        parametrize_query: bool,
    ) -> str:
        if parametrize_query:
            return UnnormalizedDirectIngestRawDataTableUpToDateView(
                region_code=region_code, raw_file_config=raw_table_config
            ).view_query
        return UnnormalizedDirectIngestRawDataTableLatestView(
            region_code=region_code,
            raw_file_config=raw_table_config,
            dataset_overrides=None,
        ).select_query_uninjected_project_id


class UnnormalizedDirectIngestPreProcessedIngestViewBuilder(
    BigQueryViewBuilder[UnnormalizedDirectIngestPreProcessedIngestView]
):
    """Factory class for building UnnormalizedDirectIngestPreProcessedIngestView"""

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
        self.materialized_location_override = None

    @property
    def file_tag(self) -> str:
        return self.ingest_view_name

    # TODO(#6314): Remove this temporary property
    @property
    def dataset_id(self) -> str:  # type: ignore
        if self.ingest_view_name != "us_pa_supervision_period_TEMP":
            raise ValueError(
                "This dataset_id property is a temporary hack in order to load the US_PA "
                "view_supervision_period query into the reference views. Should not be accessed "
                "outside of this hack."
            )
        return "reference_views"

    # pylint: disable=unused-argument
    def _build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> UnnormalizedDirectIngestPreProcessedIngestView:
        """Builds an instance of a UnnormalizedDirectIngestPreProcessedIngestView with the provided args."""
        return UnnormalizedDirectIngestPreProcessedIngestView(
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
                config=UnnormalizedDirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.PARAMETERIZED,
                )
            )
        )
        print(
            "********************************* LATEST *********************************"
        )
        print(
            view.expanded_view_query(
                config=UnnormalizedDirectIngestPreProcessedIngestView.QueryStructureConfig(
                    raw_table_view_type=RawTableViewType.LATEST,
                )
            )
        )

    def should_build(self) -> bool:
        return True
