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
import datetime
import re
import string
from enum import Enum
from types import ModuleType
from typing import List, Optional, Set

import attr
import pytz

from recidiviz.big_query.big_query_query_builder import (
    PROJECT_ID_KEY,
    BigQueryQueryBuilder,
)
from recidiviz.big_query.big_query_utils import datetime_clause
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawTableColumnInfo,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
)
from recidiviz.ingest.direct.views.raw_table_query_builder import RawTableQueryBuilder
from recidiviz.utils import metadata

UPDATE_DATETIME_PARAM_NAME = "update_timestamp"

CREATE_TEMP_TABLE_REGEX = re.compile(r"CREATE\s+((TEMP|TEMPORARY)\s+)TABLE")

CURRENT_DATE_REGEX = re.compile(r"CURRENT_DATE\(|current_date\(")


class RawFileHistoricalRowsFilterType(Enum):
    # Return all rows we have ever received for this table, even if they have since
    # been updated with a newer version.
    ALL = "ALL"

    # Returns rows in the latest version of the table
    LATEST = "LATEST"


# TODO(#29997) Make is_code_file a top level property
@attr.define(kw_only=True)
class DirectIngestViewRawFileDependency:
    """
    Contains information about a raw table that is a dependency of an ingest view.

    For example, an ingest view query with text "SELECT * FROM {myTable};"
    would have this class contain information for expanding "{myTable}" into a real
    SQL statement to be queried in BigQuery.
    """

    # Regex matching the raw table dependency format args allowed in ingest view
    # definition (i.e the text between the brackets of '{myRawTable}').
    RAW_TABLE_DEPENDENCY_REGEX = r"^(?P<raw_file_tag>\w+)(@(?P<filter_info_str>\w+))?$"

    raw_file_config: DirectIngestRawFileConfig
    filter_type: RawFileHistoricalRowsFilterType
    raw_table_dependency_arg_name: str

    def __attrs_post_init__(self) -> None:
        if self.raw_file_config.is_undocumented:
            raise ValueError(
                f"Cannot use undocumented raw file [{self.file_tag}] as a dependency "
                f"in an ingest view."
            )

    @property
    def file_tag(self) -> str:
        return self.raw_file_config.file_tag

    @property
    def current_columns(self) -> List[RawTableColumnInfo]:
        return self.raw_file_config.current_columns

    @property
    def filter_to_latest(self) -> bool:
        return self.filter_type is RawFileHistoricalRowsFilterType.LATEST

    @classmethod
    def from_raw_table_dependency_arg_name(
        cls,
        raw_table_dependency_arg_name: str,
        region_raw_table_config: DirectIngestRegionRawFileConfig,
    ) -> "DirectIngestViewRawFileDependency":
        """Parses a raw table format arg string (e.g. the text inside the brackets of
        '{myTable}') into a DirectIngestViewRawFileDependency.
        """
        match = re.match(cls.RAW_TABLE_DEPENDENCY_REGEX, raw_table_dependency_arg_name)
        if not match:
            raise ValueError(
                f"Found raw table dependency format arg "
                f"[{raw_table_dependency_arg_name}] which does not match the expected "
                f"pattern."
            )
        raw_file_tag = match.group("raw_file_tag")
        filter_info_str = match.group("filter_info_str")
        if raw_file_tag not in region_raw_table_config.raw_file_configs:
            raise ValueError(f"Found unexpected raw table tag [{raw_file_tag}]")

        raw_file_config = region_raw_table_config.raw_file_configs[raw_file_tag]

        if not filter_info_str:
            filter_type = RawFileHistoricalRowsFilterType.LATEST
        else:
            try:
                filter_type = RawFileHistoricalRowsFilterType(filter_info_str)
            except ValueError as e:
                raise ValueError(
                    f"Found unexpected filter info string [{filter_info_str}] on raw "
                    f"table dependency [{raw_table_dependency_arg_name}]"
                ) from e

        return DirectIngestViewRawFileDependency(
            raw_file_config=raw_file_config,
            filter_type=filter_type,
            raw_table_dependency_arg_name=raw_table_dependency_arg_name,
        )


class DirectIngestViewQueryBuilder:
    """Class for building SQL queries, that can be used to generate ingest view
    results in the `us_xx_ingest_view_results*` datasets.
    """

    @attr.s
    class QueryStructureConfig:
        """Configuration for how to structure the expanded view query with hydrated raw table views."""

        # If set, the raw data queries will only return rows received on or before this datetime.
        raw_data_datetime_upper_bound: Optional[datetime.datetime] = attr.ib()

        # The source of the raw data for the query
        raw_data_source_instance: DirectIngestInstance = attr.ib()

    WITH_PREFIX = "WITH"
    SUBQUERY_INDENT = "    "

    def __init__(
        self,
        *,
        region: str,
        ingest_view_name: str,
        view_query_template: str,
        region_module: ModuleType = regions,
    ):
        """Builds a view for holding direct ingest pre-processing SQL queries, that can be used to export files for
        import into our Postgres DB.

        Args:
            region: (str) The region this view corresponds to.
            ingest_view_name: (str) The name of the view.
            view_query_template: (str) The template for the query, formatted for hydration of raw table views.
            region_module: (ModuleType) Module containing all region raw data config files.
        """
        self._region_code = region
        self._raw_table_dependency_configs: Optional[
            List[DirectIngestViewRawFileDependency]
        ] = None
        self._query_builder = BigQueryQueryBuilder(
            parent_address_overrides=None, parent_address_formatter_provider=None
        )
        self.ingest_view_name = ingest_view_name

        self._view_query_template = view_query_template
        self._region_module = region_module

        if re.search(CREATE_TEMP_TABLE_REGEX, view_query_template):
            raise ValueError(
                "Found CREATE TEMP TABLE clause in this query - ingest views cannot contain CREATE clauses."
            )

        if re.search(CURRENT_DATE_REGEX, view_query_template):
            raise ValueError(
                "Found CURRENT_DATE function in this query - ingest views cannot contain CURRENT_DATE functions. "
                f"Consider using @{UPDATE_DATETIME_PARAM_NAME} instead."
            )

    @property
    def raw_table_dependency_configs(self) -> List[DirectIngestViewRawFileDependency]:
        """
        Configs for any raw tables that this view's query depends on.

        Raw table references within an ingest view should only be
        raw table names "{my_table}" for the latest rows or with
        the @ALL suffix for all rows "{my_table@ALL}"
        """
        if self._raw_table_dependency_configs is None:
            region_raw_table_config = get_region_raw_file_config(
                self._region_code, self._region_module
            )
            self._raw_table_dependency_configs = []
            raw_table_dependency_strs = sorted(
                {
                    field_name
                    for _, field_name, _, _ in string.Formatter().parse(
                        self._view_query_template
                    )
                    if field_name is not None and field_name != PROJECT_ID_KEY
                }
            )
            for raw_table_dependency_str in raw_table_dependency_strs:
                self._raw_table_dependency_configs.append(
                    DirectIngestViewRawFileDependency.from_raw_table_dependency_arg_name(
                        raw_table_dependency_str, region_raw_table_config
                    )
                )
        return self._raw_table_dependency_configs

    @property
    def raw_data_table_dependency_file_tags(self) -> Set[str]:
        """Returns the file tags of all the raw data tables this view query depends on."""
        return {
            raw_file_dependency.raw_file_config.file_tag
            for raw_file_dependency in self.raw_table_dependency_configs
        }

    def build_and_print(
        self,
        raw_data_source_instance: DirectIngestInstance = DirectIngestInstance.PRIMARY,
        date_bounded: bool = False,
    ) -> None:
        """For local testing, prints out either the date-bounded or the latest version of the view's query."""
        if date_bounded:
            print(
                "/****************************** DATE BOUNDED ******************************/"
            )
            print(
                self.build_query(
                    query_structure_config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                        raw_data_source_instance=raw_data_source_instance,
                        # TZ information was causing this to not work on BigQuery when printed here.
                        # However, having no TZ information defaults to UTC.
                        # https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime
                        raw_data_datetime_upper_bound=datetime.datetime.now(
                            tz=pytz.UTC
                        ).replace(tzinfo=None),
                    )
                )
            )
        else:
            print(
                "/********************************* LATEST *********************************/"
            )
            print(
                self.build_query(
                    query_structure_config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                        raw_data_source_instance=raw_data_source_instance,
                        raw_data_datetime_upper_bound=None,
                    )
                )
            )

    def _table_subbquery_name_and_description(
        self,
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
        config: "DirectIngestViewQueryBuilder.QueryStructureConfig",
    ) -> tuple[str, str]:
        """The name for the expanded subquery on this raw table."""
        if watermark := config.raw_data_datetime_upper_bound:
            comment_sfx = f" received on or before {watermark}"
        else:
            comment_sfx = ""
        file_tag = raw_table_dependency_config.file_tag
        if (
            raw_table_dependency_config.filter_type
            == RawFileHistoricalRowsFilterType.LATEST
        ):
            cte_name_suffix = ""
            comment = f"-- Pulls the latest data from {file_tag}{comment_sfx}"
        else:
            cte_name_suffix = "__ALL"
            comment = f"-- Pulls all rows from {file_tag}{comment_sfx}"
        return f"{file_tag}{cte_name_suffix}_generated_view", comment

    def _raw_table_subquery_clause(
        self, config: "DirectIngestViewQueryBuilder.QueryStructureConfig"
    ) -> str:
        """
        Generates the portion of the ingest view query that pulls from
        raw data table view CTES (WITH raw_table_view AS ...)
        """
        table_subquery_clause = ",\n".join(
            self._get_table_subquery_str(config, raw_table_config)
            for raw_table_config in self.raw_table_dependency_configs
        )
        return f"{self.WITH_PREFIX}\n{table_subquery_clause}"

    def _get_full_query_template(
        self, config: "DirectIngestViewQueryBuilder.QueryStructureConfig"
    ) -> str:
        """Returns the full, formatted ingest view query template that can be injected with format args."""
        query_template = self._view_query_template.strip()
        raw_table_subquery_clause = self._raw_table_subquery_clause(config)
        if query_template.startswith(self.WITH_PREFIX):
            raw_table_subquery_clause += ","
        full_query = f"{raw_table_subquery_clause}\n{query_template.removeprefix(self.WITH_PREFIX).lstrip()}"
        return full_query.rstrip().rstrip(";") + ";"

    def build_query(
        self,
        query_structure_config: "DirectIngestViewQueryBuilder.QueryStructureConfig",
    ) -> str:
        """
        Formats this view's template according to the provided config, with expanded subqueries for each raw table
        dependency.
        """
        full_query_template = self._get_full_query_template(
            config=query_structure_config
        )

        format_args = {}
        for dependency_config in self.raw_table_dependency_configs:
            table_name, _ = self._table_subbquery_name_and_description(
                dependency_config, query_structure_config
            )
            format_args[dependency_config.raw_table_dependency_arg_name] = table_name

        query = self._query_builder.build_query(
            project_id=metadata.project_id(),
            query_template=full_query_template,
            query_format_kwargs=format_args,
        )

        # We manually hydrate @update_timestamp parameters that may be defined in the
        # main body of an ingest view query.
        if query_structure_config.raw_data_datetime_upper_bound:
            query = query.replace(
                f"@{UPDATE_DATETIME_PARAM_NAME}",
                f"{datetime_clause(query_structure_config.raw_data_datetime_upper_bound)}",
            )
        else:
            query = query.replace(
                f"@{UPDATE_DATETIME_PARAM_NAME}", "CURRENT_DATE('US/Eastern')"
            )

        return query.strip()

    def _get_table_subquery_str(
        self,
        query_structure_config: "DirectIngestViewQueryBuilder.QueryStructureConfig",
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
    ) -> str:
        """Returns an expanded subquery on this raw table in the form 'subquery_name AS (...)'."""
        date_bounded_query = self._date_bounded_query_for_raw_table(
            config=query_structure_config,
            raw_table_dependency_config=raw_table_dependency_config,
        )
        date_bounded_query = date_bounded_query.strip("\n")
        indented_date_bounded_query = self.SUBQUERY_INDENT + date_bounded_query.replace(
            "\n", "\n" + self.SUBQUERY_INDENT
        )

        indented_date_bounded_query = indented_date_bounded_query.replace(
            f"\n{self.SUBQUERY_INDENT}\n", "\n\n"
        )
        table_subquery_name, description = self._table_subbquery_name_and_description(
            raw_table_dependency_config, query_structure_config
        )
        return f"{description}\n{table_subquery_name} AS (\n{indented_date_bounded_query}\n)"

    def _date_bounded_query_for_raw_table(
        self,
        config: "DirectIngestViewQueryBuilder.QueryStructureConfig",
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
    ) -> str:
        project_id = metadata.project_id()

        if (
            not config.raw_data_datetime_upper_bound
            and raw_table_dependency_config.filter_to_latest
        ):
            # If there is no bound and we are filtering to the latest version of each
            # row, we can query directly from the `latest` view for convenience.
            return (
                DirectIngestRawDataTableLatestViewBuilder(
                    region_code=self._region_code,
                    raw_data_source_instance=config.raw_data_source_instance,
                    raw_file_config=raw_table_dependency_config.raw_file_config,
                    regions_module=self._region_module,
                    filter_to_only_documented_columns=True,
                )
                .table_for_query.to_project_specific_address(project_id)
                .select_query()
            )

        return RawTableQueryBuilder(
            project_id=project_id,
            region_code=self._region_code,
            raw_data_source_instance=config.raw_data_source_instance,
        ).build_query(
            raw_file_config=raw_table_dependency_config.raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=config.raw_data_datetime_upper_bound,
            filter_to_latest=raw_table_dependency_config.filter_to_latest,
            filter_to_only_documented_columns=True,
        )
