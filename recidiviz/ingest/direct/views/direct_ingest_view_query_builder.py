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
from enum import Enum, auto
from types import ModuleType
from typing import List, Optional, Set

import attr
import pytz

from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
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
from recidiviz.utils.string import StrictStringFormatter

UPDATE_DATETIME_PARAM_NAME = "update_timestamp"

CREATE_TEMP_TABLE_REGEX = re.compile(r"CREATE\s+((TEMP|TEMPORARY)\s+)TABLE")

CURRENT_DATE_REGEX = re.compile(r"CURRENT_DATE\(|current_date\(")

DESTINATION_TABLE_QUERY_FORMAT = """
DROP TABLE IF EXISTS `{{project_id}}.{dataset_id}.{table_id}`;
CREATE TABLE `{{project_id}}.{dataset_id}.{table_id}`
OPTIONS(
  -- Data in this table will be deleted after 24 hours
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (

{select_query_clause}

);
"""

DESTINATION_TEMP_TABLE_QUERY_FORMAT = """
CREATE TEMP TABLE {table_id} AS (

{select_query_clause}

);
"""


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


class RawFileHistoricalRowsFilterType(Enum):
    # Return all rows we have ever received for this table, even if they have since
    # been updated with a newer version.
    ALL = "ALL"

    # Returns rows in the latest version of the table
    LATEST = "LATEST"


# TODO(#29997) Make is_code_file a top level property
@attr.define(kw_only=True)
class DirectIngestViewRawFileDependency:
    """Class containing information about a raw table that is a dependency of an ingest
    view.

    For example, for the ingest view text: "SELECT * FROM {myTable};", this class
    contains information for how to expand {myTable} into a real SELECT clause against
    BigQuery.
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
    def columns(self) -> List[RawTableColumnInfo]:
        return self.raw_file_config.columns

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

        # If set, the raw data queries will only return rows received on or before this
        # datetime.
        raw_data_datetime_upper_bound: Optional[datetime.datetime] = attr.ib()

        # The source of the raw data for the query
        raw_data_source_instance: DirectIngestInstance = attr.ib()

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

        # If set, the ingest view does not load any rows.
        limit_zero: bool = attr.ib(default=False)

        def __attrs_post_init__(self) -> None:
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
        """Configs for any raw tables that this view's query depends on."""
        if self._raw_table_dependency_configs is None:
            region_raw_table_config = get_region_raw_file_config(
                self._region_code, self._region_module
            )
            self._raw_table_dependency_configs = self._get_raw_table_dependency_configs(
                region_raw_table_config
            )

        return self._raw_table_dependency_configs

    @property
    def raw_data_table_dependency_file_tags(self) -> Set[str]:
        """Returns the file tags of all the raw data tables this view query depends on."""
        return {
            raw_file_dependency.raw_file_config.file_tag
            for raw_file_dependency in self.raw_table_dependency_configs
        }

    def build_query(
        self, config: "DirectIngestViewQueryBuilder.QueryStructureConfig"
    ) -> str:
        """Formats this view's template according to the provided config, with expanded subqueries for each raw table
        dependency."""
        query = self._format_expanded_view_query(config=config)
        return self._query_builder.build_query(
            project_id=metadata.project_id(),
            query_template=query,
            query_format_kwargs={},
        )

    def build_and_print(
        self,
        raw_data_source_instance: DirectIngestInstance = DirectIngestInstance.PRIMARY,
    ) -> None:
        """For local testing, prints out the date-bounded and latest versions of the
        view's query.
        """
        print(
            "****************************** DATE BOUNDED ******************************"
        )
        print(
            self.build_query(
                config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                    raw_data_source_instance=raw_data_source_instance,
                    raw_data_datetime_upper_bound=datetime.datetime.now(tz=pytz.UTC),
                )
            )
        )
        print(
            "********************************* LATEST *********************************"
        )
        print(
            self.build_query(
                config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                    raw_data_source_instance=raw_data_source_instance,
                    raw_data_datetime_upper_bound=None,
                )
            )
        )

    @staticmethod
    def _table_subbquery_name(
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
        prefix: Optional[str],
    ) -> str:
        """The name for the expanded subquery on this raw table."""

        prefix = prefix or ""
        dependency_name = raw_table_dependency_config.file_tag
        filter_type = raw_table_dependency_config.filter_type
        if filter_type != RawFileHistoricalRowsFilterType.LATEST:
            dependency_name += f"__{filter_type.value}"
        return f"{prefix}{dependency_name}_generated_view"

    @staticmethod
    def add_limit_zero_suffix(query: str) -> str:
        query = query.rstrip().rstrip(";")
        return f"{query}\nLIMIT 0;"

    def _raw_table_subquery_clause(
        self, config: "DirectIngestViewQueryBuilder.QueryStructureConfig"
    ) -> str:
        """Returns the portion of the script that generates the raw table view queries, either as a list of
        `CREATE TEMP TABLE` statements or a list of WITH subqueries.
        """
        table_subquery_strs = []
        for raw_table_config in self.raw_table_dependency_configs:
            table_subquery_strs.append(
                self._get_table_subquery_str(config, raw_table_config)
            )

        table_subquery_clause = ",\n".join(table_subquery_strs)
        return f"{self.WITH_PREFIX}\n{table_subquery_clause}"

    def _get_select_query_clause(
        self, config: "DirectIngestViewQueryBuilder.QueryStructureConfig"
    ) -> str:
        """Returns the final SELECT statement that produces the results for this ingest
        view query. Pulls in raw table data as WITH subqueries.
        """
        view_query_template = self._view_query_template.strip()

        raw_table_subquery_clause = self._raw_table_subquery_clause(config)
        if view_query_template.startswith(self.WITH_PREFIX):
            view_query_template = view_query_template[len(self.WITH_PREFIX) :].lstrip()
            raw_table_subquery_clause = raw_table_subquery_clause + ","

        select_query_clause = f"{raw_table_subquery_clause}\n{view_query_template}"
        if config.limit_zero:
            select_query_clause = self.add_limit_zero_suffix(query=select_query_clause)
        select_query_clause = select_query_clause.rstrip().rstrip(";")
        return select_query_clause

    def _get_full_query_template(
        self, config: "DirectIngestViewQueryBuilder.QueryStructureConfig"
    ) -> str:
        """Returns the full, formatted ingest view query template that can be injected with format args."""
        select_query_clause = self._get_select_query_clause(config=config)

        if config.destination_table_type == DestinationTableType.PERMANENT_EXPIRING:
            return StrictStringFormatter().format(
                DESTINATION_TABLE_QUERY_FORMAT,
                dataset_id=config.destination_dataset_id,
                table_id=config.destination_table_id,
                select_query_clause=select_query_clause,
            )
        if config.destination_table_type == DestinationTableType.TEMPORARY:
            return StrictStringFormatter().format(
                DESTINATION_TEMP_TABLE_QUERY_FORMAT,
                table_id=config.destination_table_id,
                select_query_clause=select_query_clause,
            )
        if config.destination_table_type == DestinationTableType.NONE:
            return f"{select_query_clause};"

        raise ValueError(
            f"Unsupported destination_table_type: [{config.destination_table_type.name}]"
        )

    def _format_expanded_view_query(
        self, config: "DirectIngestViewQueryBuilder.QueryStructureConfig"
    ) -> str:
        """Formats the given template with expanded subqueries for each raw table dependency according to the given
        config. Does not hydrate the project_id so the result of this function can be passed as a template to the
        superclass constructor.
        """
        full_query_template = self._get_full_query_template(config=config)

        format_args = {}
        for raw_table_dependency_config in self.raw_table_dependency_configs:
            format_args[
                raw_table_dependency_config.raw_table_dependency_arg_name
            ] = self._table_subbquery_name(
                raw_table_dependency_config, config.raw_table_subquery_name_prefix
            )

        query = self._query_builder.build_query(
            project_id=metadata.project_id(),
            query_template=full_query_template,
            query_format_kwargs=format_args,
        )

        # We manually hydrate @update_timestamp parameters that may be defined in the
        # main body of an ingest view query.
        if config.raw_data_datetime_upper_bound:
            query = query.replace(
                f"@{UPDATE_DATETIME_PARAM_NAME}",
                f"{datetime_clause(config.raw_data_datetime_upper_bound)}",
            )
        else:
            query = query.replace(
                f"@{UPDATE_DATETIME_PARAM_NAME}", "CURRENT_DATE('US/Eastern')"
            )

        return query.strip()

    # TODO(#29272) Self document generated CTEs
    def _get_table_subquery_str(
        self,
        config: "DirectIngestViewQueryBuilder.QueryStructureConfig",
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
    ) -> str:
        """Returns an expanded subquery on this raw table in the form 'subquery_name AS (...)'."""
        date_bounded_query = self._date_bounded_query_for_raw_table(
            config=config,
            raw_table_dependency_config=raw_table_dependency_config,
        )
        date_bounded_query = date_bounded_query.strip("\n")
        indented_date_bounded_query = self.SUBQUERY_INDENT + date_bounded_query.replace(
            "\n", "\n" + self.SUBQUERY_INDENT
        )

        indented_date_bounded_query = indented_date_bounded_query.replace(
            f"\n{self.SUBQUERY_INDENT}\n", "\n\n"
        )
        table_subquery_name = self._table_subbquery_name(
            raw_table_dependency_config, config.raw_table_subquery_name_prefix
        )
        return f"{table_subquery_name} AS (\n{indented_date_bounded_query}\n)"

    def _get_raw_table_dependency_configs(
        self, region_raw_table_config: DirectIngestRegionRawFileConfig
    ) -> List[DirectIngestViewRawFileDependency]:
        """Returns a sorted list of configs for all raw files this query depends on."""
        raw_table_dependency_strs = self._parse_raw_table_dependencies(
            self._view_query_template
        )
        raw_table_dependency_configs = []
        for raw_table_dependency_str in raw_table_dependency_strs:
            raw_table_dependency_configs.append(
                DirectIngestViewRawFileDependency.from_raw_table_dependency_arg_name(
                    raw_table_dependency_str, region_raw_table_config
                )
            )
        return raw_table_dependency_configs

    @staticmethod
    def _parse_raw_table_dependencies(view_query_template: str) -> List[str]:
        """Parses and returns all format args in the view query template and returns as
        a sorted list.

        These format args should only be raw table names or raw table names with a
        suffix indicating that they should be queried in a specific way (e.g.
        "my_table@ALL").
        """
        dependencies_set = {
            field_name
            for _, field_name, _, _ in string.Formatter().parse(view_query_template)
            if field_name is not None
        }
        return sorted(dependencies_set)

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
