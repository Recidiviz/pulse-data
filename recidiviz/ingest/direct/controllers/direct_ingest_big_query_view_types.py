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
import string
from typing import List, Optional

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRawFileConfig, \
    DirectIngestRawFileImportManager, DirectIngestRegionRawFileConfig, _FILE_ID_COL_NAME, _UPDATE_DATETIME_COL_NAME
from recidiviz.ingest.direct.query_utils import get_region_raw_file_config

UPDATE_DATETIME_PARAM_NAME = "update_timestamp"

# A parametrized query for looking at the most recent row for each primary key, among rows with update datetimes
# before a certain date.
RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE = f"""
WITH rows_with_recency_rank AS (
    SELECT 
        * {{except_clause}}, {{datetime_cols_clause}}
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
        * {{except_clause}}, {{datetime_cols_clause}}
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
        * {except_clause}, {datetime_cols_clause}
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
        * {except_clause}, {datetime_cols_clause}
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
        ) AS {col_name},"""


class DirectIngestRawDataTableBigQueryView(BigQueryView):
    """A base class for BigQuery views that give us a view of a region's raw data on a given date."""
    def __init__(self, *,
                 project_id: str = None,
                 region_code: str,
                 view_id: str,
                 view_query_template: str,
                 raw_file_config: DirectIngestRawFileConfig):
        if not raw_file_config.primary_key_cols:
            raise ValueError(f'Empty primary key list in raw file config with tag [{raw_file_config.file_tag}] during '
                             f'construction of DirectIngestRawDataTableBigQueryView')
        view_dataset_id = f'{region_code.lower()}_raw_data_up_to_date_views'
        raw_table_dataset_id = DirectIngestRawFileImportManager.raw_tables_dataset_for_region(region_code)
        except_clause = self._except_clause_for_config(raw_file_config)
        datetime_cols_clause = self._datetime_cols_clause_for_config(raw_file_config)
        supplemental_order_by_clause = self._supplemental_order_by_clause_for_config(raw_file_config)
        super().__init__(project_id=project_id,
                         dataset_id=view_dataset_id,
                         view_id=view_id,
                         view_query_template=view_query_template,
                         raw_table_dataset_id=raw_table_dataset_id,
                         raw_table_name=raw_file_config.file_tag,
                         raw_table_primary_key_str=raw_file_config.primary_key_str,
                         except_clause=except_clause,
                         datetime_cols_clause=datetime_cols_clause,
                         supplemental_order_by_clause=supplemental_order_by_clause)

    @staticmethod
    def _supplemental_order_by_clause_for_config(raw_file_config: DirectIngestRawFileConfig):
        if not raw_file_config.supplemental_order_by_clause:
            return ''

        supplemental_order_by_clause = raw_file_config.supplemental_order_by_clause.strip()
        if not supplemental_order_by_clause.startswith(','):
            return ', ' + supplemental_order_by_clause

        return supplemental_order_by_clause

    @staticmethod
    def _except_clause_for_config(raw_file_config: DirectIngestRawFileConfig) -> str:
        # TODO(#3020): Update the raw data yaml format to allow for us to specify other columns that should always be
        #  excluded for the purposes of diffing (e.g. update date cols that change with every new import).
        except_cols = raw_file_config.datetime_cols + [_FILE_ID_COL_NAME, _UPDATE_DATETIME_COL_NAME]
        except_cols_str = ', '.join(except_cols)
        return f'EXCEPT ({except_cols_str})'

    @staticmethod
    def _datetime_cols_clause_for_config(raw_file_config: DirectIngestRawFileConfig) -> str:
        if not raw_file_config.datetime_cols:
            return ''

        formatted_clauses = [
            DATETIME_COL_NORMALIZATION_TEMPLATE.format(col_name=col_name) for col_name in raw_file_config.datetime_cols
        ]
        return ''.join(formatted_clauses)


class DirectIngestRawDataTableLatestView(DirectIngestRawDataTableBigQueryView):
    """A BigQuery view with a query for the given |raw_table_name|, which when used will load the most up-to-date values
    of all rows in that table.
    """
    def __init__(self,
                 *,
                 project_id: str = None,
                 region_code: str,
                 raw_file_config: DirectIngestRawFileConfig):
        view_id = f'{raw_file_config.file_tag}_latest'
        view_query_template = RAW_DATA_LATEST_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE \
            if raw_file_config.always_historical_export else RAW_DATA_LATEST_VIEW_QUERY_TEMPLATE
        super().__init__(project_id=project_id,
                         region_code=region_code,
                         view_id=view_id,
                         view_query_template=view_query_template,
                         raw_file_config=raw_file_config)


# NOTE: BigQuery does not support parametrized queries for views, so we can't actually upload this as a view until this
# issue is resolved: https://issuetracker.google.com/issues/35905221. For now, we construct it like a BigQueryView, but
# just use the view_query field to get a query we can execute to pull data in direct ingest.
class DirectIngestRawDataTableUpToDateView(DirectIngestRawDataTableBigQueryView):
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
        view_query_template = RAW_DATA_UP_TO_DATE_HISTORICAL_FILE_VIEW_QUERY_TEMPLATE \
            if raw_file_config.always_historical_export else RAW_DATA_UP_TO_DATE_VIEW_QUERY_TEMPLATE
        super().__init__(project_id=project_id,
                         region_code=region_code,
                         view_id=view_id,
                         view_query_template=view_query_template,
                         raw_file_config=raw_file_config)


class DirectIngestPreProcessedIngestView(BigQueryView):
    """Class for holding direct ingest pre-processing SQL queries, that can be used to export files for import into our
    Postgres DB.
    """

    WITH_PREFIX = 'WITH'
    SUBQUERY_INDENT = '    '

    def __init__(self, *,
                 ingest_view_name: str,
                 view_query_template: str,
                 region_raw_table_config: DirectIngestRegionRawFileConfig,
                 order_by_cols: Optional[str],
                 is_detect_row_deletion_view: bool,
                 primary_key_tables_for_entity_deletion: List[str]):
        DirectIngestPreProcessedIngestView._validate_order_by(
            ingest_view_name=ingest_view_name, view_query_template=view_query_template)

        region_code = region_raw_table_config.region_code
        self._order_by_cols = order_by_cols
        raw_table_dependency_configs = self._get_raw_table_dependency_configs(view_query_template,
                                                                              region_raw_table_config)

        latest_view_query = self._format_expanded_view_query(region_code=region_code,
                                                             raw_table_dependency_configs=raw_table_dependency_configs,
                                                             view_query_template=view_query_template,
                                                             order_by_cols=order_by_cols,
                                                             parametrize_query=False)

        dataset_id = f'{region_code.lower()}_ingest_views'
        super().__init__(dataset_id=dataset_id,
                         view_id=ingest_view_name,
                         view_query_template=latest_view_query)

        self._raw_table_dependency_configs = raw_table_dependency_configs
        date_parametrized_view_query = self._format_expanded_view_query(
            region_code=region_code,
            raw_table_dependency_configs=raw_table_dependency_configs,
            view_query_template=view_query_template,
            order_by_cols=order_by_cols,
            parametrize_query=True)
        self._date_parametrized_view_query = date_parametrized_view_query.format(**self.
                                                                                 _query_format_args_with_project_id())
        self._is_detect_row_deletion_view = is_detect_row_deletion_view
        if self._is_detect_row_deletion_view:
            self._validate_can_detect_row_deletion(
                raw_configs=raw_table_dependency_configs,
                ingest_view_name=ingest_view_name,
                primary_key_tables_for_entity_deletion=primary_key_tables_for_entity_deletion)

    @property
    def file_tag(self) -> str:
        """The file tag that should be written to any file export of this query."""
        return self.view_id

    @property
    def raw_table_dependency_configs(self) -> List[DirectIngestRawFileConfig]:
        """Configs for any raw tables that this view's query depends on."""
        return self._raw_table_dependency_configs

    @property
    def latest_view_query(self) -> str:
        """Non-parametrized query on the latest version of each raw table."""
        return self.view_query

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

    def date_parametrized_view_query(self, param_name: Optional[str] = None) -> str:
        """Parametrized query on the version of each raw table on a given date. If provided, the parameter name for the
        max update date will have the provided |param_name|, otherwise the parameter name will be the default
        UPDATE_DATETIME_PARAM_NAME.
        """
        if not param_name:
            return self._date_parametrized_view_query

        return self._date_parametrized_view_query.replace(f'@{UPDATE_DATETIME_PARAM_NAME}', f'@{param_name}')

    @staticmethod
    def _table_subbquery_name(raw_table_config: DirectIngestRawFileConfig) -> str:
        """The name for the expanded subquery on this raw table."""
        return f'{raw_table_config.file_tag}_generated_view'

    @staticmethod
    def add_order_by_suffix(query: str, order_by_cols: Optional[str]):
        if order_by_cols:
            query = query.rstrip().rstrip(';')
            query = f'{query} \nORDER BY {order_by_cols};'
        return query

    @classmethod
    def _format_expanded_view_query(cls,
                                    region_code: str,
                                    raw_table_dependency_configs: List[DirectIngestRawFileConfig],
                                    view_query_template: str,
                                    order_by_cols: Optional[str],
                                    parametrize_query: bool) -> str:
        """Formats the given template with expanded subqueries for each raw table dependency."""
        table_subquery_strs = []
        format_args = {}
        for raw_table_config in raw_table_dependency_configs:
            table_subquery_strs.append(cls._get_table_subquery_str(region_code, raw_table_config, parametrize_query))
            format_args[raw_table_config.file_tag] = cls._table_subbquery_name(raw_table_config)

        table_subquery_clause = ',\n'.join(table_subquery_strs)

        view_query_template = view_query_template.strip()
        if view_query_template.startswith(cls.WITH_PREFIX):
            view_query_template = view_query_template[len(cls.WITH_PREFIX):].lstrip()
            table_subquery_clause = table_subquery_clause + ','

        view_query_template = f'{cls.WITH_PREFIX}\n{table_subquery_clause}\n{view_query_template}'
        view_query_template = cls.add_order_by_suffix(query=view_query_template, order_by_cols=order_by_cols)

        # We don't want to inject the project_id outside of the BigQueryView initializer
        return cls._format_view_query_without_project_id(view_query_template, **format_args)

    @classmethod
    def _get_table_subquery_str(cls,
                                region_code: str,
                                raw_table_config: DirectIngestRawFileConfig,
                                parametrize_query: bool) -> str:
        """Returns an expanded subquery on this raw table in the form 'subquery_name AS (...)'."""
        date_bounded_query = cls._date_bounded_query_for_raw_table(
            region_code=region_code, raw_table_config=raw_table_config, parametrize_query=parametrize_query)
        date_bounded_query = date_bounded_query.strip('\n')
        indented_date_bounded_query = cls.SUBQUERY_INDENT + date_bounded_query.replace('\n',
                                                                                       '\n' + cls.SUBQUERY_INDENT)

        indented_date_bounded_query = indented_date_bounded_query.replace(f'\n{cls.SUBQUERY_INDENT}\n', '\n\n')
        table_subquery_name = cls._table_subbquery_name(raw_table_config)
        return f'{table_subquery_name} AS (\n{indented_date_bounded_query}\n)'

    @classmethod
    def _get_raw_table_dependency_configs(
            cls,
            view_query_template: str,
            region_raw_table_config: DirectIngestRegionRawFileConfig) -> List[DirectIngestRawFileConfig]:
        """Returns a sorted list of configs for all raw files this query depends on."""
        raw_table_dependencies = cls._parse_raw_table_dependencies(view_query_template)
        raw_table_dependency_configs = []
        for raw_table_tag in raw_table_dependencies:
            if raw_table_tag not in region_raw_table_config.raw_file_configs:
                raise ValueError(f'Found unexpected raw table tag [{raw_table_tag}]')
            if not region_raw_table_config.raw_file_configs[raw_table_tag].primary_key_cols:
                raise ValueError(f'Empty primary key list in raw file config with tag [{raw_table_tag}]')
            raw_table_dependency_configs.append(region_raw_table_config.raw_file_configs[raw_table_tag])
        return raw_table_dependency_configs

    @staticmethod
    def _parse_raw_table_dependencies(view_query_template: str) -> List[str]:
        """Parses and returns all format args in the view query template (should be only raw table names) and returns as
        a sorted list."""
        dependencies_set = {field_name
                            for _, field_name, _, _ in string.Formatter().parse(view_query_template)
                            if field_name is not None}
        return sorted(dependencies_set)

    @staticmethod
    def _date_bounded_query_for_raw_table(region_code: str,
                                          raw_table_config: DirectIngestRawFileConfig,
                                          parametrize_query: bool) -> str:
        if parametrize_query:
            return DirectIngestRawDataTableUpToDateView(region_code=region_code,
                                                        raw_file_config=raw_table_config).view_query
        return DirectIngestRawDataTableLatestView(region_code=region_code,
                                                  raw_file_config=raw_table_config).select_query_uninjected_project_id

    @staticmethod
    def _validate_order_by(ingest_view_name: str, view_query_template: str):
        query = view_query_template.upper()
        final_sub_query = query.split('FROM')[-1]
        order_by_count = final_sub_query.count('ORDER BY')
        if order_by_count:
            raise ValueError(
                f'Found ORDER BY after the final FROM statement in the SQL view_query_template for '
                f'{ingest_view_name}. Please ensure that all ordering of the final query is done by specifying '
                f'DirectIngestPreProcessedIngestView.order_by_cols instead of putting an ORDER BY '
                f'clause in DirectIngestPreProcessingIngestView.view_query_template. If this ORDER BY is a result'
                f'of an inline subquery in the final SELECT statement, please consider moving alias-ing the subquery '
                f'or otherwise refactoring the query so no ORDER BY statements occur after the final `FROM`')

    @staticmethod
    def _validate_can_detect_row_deletion(ingest_view_name,
                                          primary_key_tables_for_entity_deletion: List[str],
                                          raw_configs: List[DirectIngestRawFileConfig]):
        if not primary_key_tables_for_entity_deletion:
            raise ValueError(f'Ingest view {ingest_view_name} was marked as `is_detect_row_deletion_view`; however no '
                             f'`primary_key_tables_for_entity_deletion` were defined. When the view is constructed, '
                             f'please specify all raw tables necessary for generating the primary key of the to-be-'
                             f'deleted entity into this ingest field.')

        raw_config_file_tags = [r.file_tag for r in raw_configs]
        for primary_key_table_name in primary_key_tables_for_entity_deletion:
            if primary_key_table_name not in raw_config_file_tags:
                raise ValueError(
                    f'Ingest view {ingest_view_name} has specified {primary_key_table_name} in '
                    f'`primary_key_tables_for_entity_deletion`, but that raw file tag was not found as a dependency. '
                    f'Please make sure all tables specified in `primary_key_tables_for_entity_deletion` appear in the '
                    f'ingest view\'s query.')

        for raw_config in raw_configs:
            if raw_config.file_tag not in primary_key_tables_for_entity_deletion:
                continue

            if not raw_config.always_historical_export:
                raise ValueError(
                    f'Ingest view {ingest_view_name} is marked as `is_detect_row_deletion_view` and has table '
                    f'{raw_config.file_tag} specified in `primary_key_tables_for_entity_deletion`; however the raw '
                    f'data file is not marked as always being exported as historically. For '
                    f'`is_detect_row_deletion_view` to be True, we must receive historical exports for all tables '
                    f'that provide the primary keys of the entity to be deleted. Please ensure that '
                    f'`primary_key_tables_for_entity_deletion`, and the raw table configs for those specified tables '
                    f'are up to date. If this is up to date, and we don\'t receive historical exports for one of the '
                    f'tables responsible for generating the to be deleted entity\'s primary key, then we cannot '
                    f'do row deletion detection.')


class DirectIngestPreProcessedIngestViewBuilder(BigQueryViewBuilder[DirectIngestPreProcessedIngestView]):
    """Factory class for building DirectIngestPreProcessedIngestView"""

    def __init__(self, *,
                 region: str,
                 ingest_view_name: str,
                 view_query_template: str,
                 order_by_cols: Optional[str],
                 is_detect_row_deletion_view: bool = False,
                 primary_key_tables_for_entity_deletion: Optional[List[str]] = None):
        self.region = region
        self.ingest_view_name = ingest_view_name
        self.view_query_template = view_query_template
        self.order_by_cols = order_by_cols
        self.is_detect_row_deletion_view = is_detect_row_deletion_view
        self.primary_key_tables_for_entity_deletion = primary_key_tables_for_entity_deletion or []

    def build(self) -> DirectIngestPreProcessedIngestView:
        """Builds an instance of a DirectIngestPreProcessedIngestView with the provided args."""
        return DirectIngestPreProcessedIngestView(
            ingest_view_name=self.ingest_view_name,
            view_query_template=self.view_query_template,
            region_raw_table_config=get_region_raw_file_config(self.region),
            order_by_cols=self.order_by_cols,
            is_detect_row_deletion_view=self.is_detect_row_deletion_view,
            primary_key_tables_for_entity_deletion=self.primary_key_tables_for_entity_deletion
        )

    def build_and_print(self):
        """For local testing, prints out the parametrized and latest versions of the view's query."""
        view = self.build()
        print('****************************** PARAMETRIZED ******************************')
        print(view.date_parametrized_view_query())
        print('********************************* LATEST *********************************')
        print(view.latest_view_query)
