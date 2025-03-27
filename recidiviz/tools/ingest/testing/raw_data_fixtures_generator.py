#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#

"""Class to generate or regenerate raw data fixtures for ingest view tests."""
import os
from functools import partial
from typing import Dict, List, Optional

from more_itertools import first
from pandas import DataFrame
from tabulate import tabulate

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewRawFileDependency,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.ingest.direct.views.raw_table_query_builder import RawTableQueryBuilder
from recidiviz.tests.ingest.direct.fixture_util import (
    fixture_path_for_raw_data_dependency,
    write_raw_fixture_dataframe_to_path,
)
from recidiviz.tools.ingest.testing.ingest_fixture_creation.randomize_fixture_data import (
    randomize_value,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation

# TODO(#40159) Delete this constant when randomization capabilities are updated
RANDOMIZED_COLUMN_PREFIX = "recidiviz_randomized_"


class RawDataFixturesGenerator:
    """Class for generating raw data fixtures for ingest view tests."""

    def __init__(
        self,
        *,
        project_id: str,
        region_code: str,
        ingest_view_tag: str,
        output_filename: str,
        root_entity_external_ids: List[str],
        root_entity_external_id_columns: List[str],
        columns_to_randomize: List[str],
        file_tags_to_load_in_full: List[str],
        randomized_values_map: Dict[str, str],
        datetime_format: str,
        overwrite: Optional[bool] = False,
        # TODO(#29994) Add functionality to get columns via a regex.
        root_entity_external_id_column_regex: Optional[str] = None,
    ):
        self.project_id = project_id
        self.state_code = StateCode(region_code.upper())
        self.ingest_view_tag = ingest_view_tag
        # We don't pass the file extension to the fixture path functions
        self.output_filename = output_filename.split(".")[0]
        self.root_entity_external_ids = root_entity_external_ids
        self.root_entity_external_id_columns = root_entity_external_id_columns
        self.root_entity_external_id_column_regex = root_entity_external_id_column_regex
        self.file_tags_to_load_in_full = file_tags_to_load_in_full
        self.randomized_values_map = randomized_values_map
        self.datetime_format = datetime_format
        self.overwrite = overwrite

        self.bq_client = BigQueryClientImpl(project_id=project_id)

        query_builder = DirectIngestViewQueryBuilderCollector(
            get_direct_ingest_region(region_code), []
        ).get_query_builder_by_view_name(ingest_view_tag)

        self.ingest_view_raw_table_dependency_configs = (
            query_builder.raw_table_dependency_configs
        )

        # TODO(#12178) Rely only on pii fields once all states have labeled PII fields.
        # TODO(#40159) Update randomization capabilities
        self.columns_to_randomize = [
            column
            for raw_config in self.ingest_view_raw_table_dependency_configs
            for column in raw_config.current_columns
            if column.is_pii or column.name in columns_to_randomize
        ]

        self.columns_to_randomize_by_table = {
            raw_config.file_tag: [
                column
                for column in raw_config.current_columns
                if column.is_pii or column.name in columns_to_randomize
            ]
            for raw_config in self.ingest_view_raw_table_dependency_configs
        }

        # Only look at primary instance to find the raw data
        self.raw_data_source_instance = DirectIngestInstance.PRIMARY
        self.query_builder = RawTableQueryBuilder(
            project_id=self.project_id,
            region_code=self.state_code.value,
            raw_data_source_instance=self.raw_data_source_instance,
        )

    def build_query_for_raw_table(
        self,
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
        root_entity_external_id_columns: List[str],
    ) -> str:
        """Builds a query that can be used to collect a sample of rows from the raw data
        table associated with the provided |raw_file_config|. Will return the most
        recent version of each row associated with the people in
        |root_entity_external_id_columns|. Datetime cols etc are NOT normalized like they are
        in *latest views (i.e. they retain the formatting in the source raw data table).
        """
        filter_by_values = ", ".join(
            f"'{id_filter}'" for id_filter in self.root_entity_external_ids
        )

        person_external_id_column = (
            root_entity_external_id_columns.pop(0)
            if root_entity_external_id_columns
            else None
        )

        id_filter_condition = (
            f"WHERE {person_external_id_column} IN ({filter_by_values})"
            if person_external_id_column
            else ""
        )

        if root_entity_external_id_columns and id_filter_condition:
            for person_external_id_column in root_entity_external_id_columns:
                id_filter_condition += (
                    f" OR {person_external_id_column} IN ({filter_by_values})"
                )

        latest_query_template = self.query_builder.build_query(
            raw_file_config=raw_table_dependency_config.raw_file_config,
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
            normalized_column_values=False,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=raw_table_dependency_config.filter_to_latest,
            filter_to_only_documented_columns=True,
        )

        return f"{latest_query_template}{id_filter_condition};"

    def validate_raw_table_exists(
        self, raw_file_config: DirectIngestRawFileConfig
    ) -> None:
        raw_table_dataset_id = raw_tables_dataset_for_region(
            state_code=self.state_code,
            instance=self.raw_data_source_instance,
            sandbox_dataset_prefix=None,
        )
        if not self.bq_client.table_exists(
            BigQueryAddress(
                dataset_id=raw_table_dataset_id, table_id=raw_file_config.file_tag
            )
        ):
            raise RuntimeError(
                f"Table [{raw_table_dataset_id}].[{raw_file_config.file_tag}] does not exist, exiting."
            )

    # TODO(#40159) Update randomization capabilities
    def randomize_column_data(
        self,
        query_results: DataFrame,
    ) -> DataFrame:
        """Given a dataframe and a list of columns to randomize, returns a dataframe with those values randomized. If the
        value already exists in the randomized_values_map, then it uses the existing randomized value. Otherwise it
        generates a random value and saves it in the randomized_values_map for the next raw table in the iteration.
        """
        original_column_order = list(query_results.columns)

        if not self.columns_to_randomize:
            return query_results

        columns_to_randomize_for_table = [
            column
            for column in original_column_order
            if any(
                column == column_info.name for column_info in self.columns_to_randomize
            )
        ]

        column_info_for_columns_to_randomize = {
            column: first(
                column_info
                for column_info in self.columns_to_randomize
                if column == column_info.name
            )
            for column in columns_to_randomize_for_table
        }

        if not columns_to_randomize_for_table:
            return query_results

        # Create a new Dataframe with only the columns and distinct values that will be randomized
        distinct_values_to_randomize = query_results[
            columns_to_randomize_for_table
        ].drop_duplicates()

        original_col_name_to_random_col_name = {
            col_name: f"{RANDOMIZED_COLUMN_PREFIX}{col_name}"
            for col_name in columns_to_randomize_for_table
        }
        random_col_name_to_original_col_name = {
            value: key for (key, value) in original_col_name_to_random_col_name.items()
        }

        def find_or_create_randomized_value(
            column_info: RawTableColumnInfo, original_value: Optional[str]
        ) -> str:
            if original_value is None:
                return ""
            if original_value not in self.randomized_values_map:
                self.randomized_values_map[original_value] = randomize_value(
                    original_value, column_info, self.datetime_format
                )
            return self.randomized_values_map[original_value]

        for column in columns_to_randomize_for_table:
            distinct_values_to_randomize[
                original_col_name_to_random_col_name[column]
            ] = distinct_values_to_randomize[column].apply(
                partial(
                    find_or_create_randomized_value,
                    column_info_for_columns_to_randomize[column],
                )
            )

        # Replace the original values with the randomized values in the original dataframe
        randomized_results = (
            query_results.merge(
                distinct_values_to_randomize,
                on=columns_to_randomize_for_table,
                how="inner",
            )
            .drop(columns=columns_to_randomize_for_table)
            .rename(columns=random_col_name_to_original_col_name)
        )
        return randomized_results[original_column_order]

    def preview_generation(self) -> None:
        """
        Displays a preview of the actions that will be performed by the generate_raw_data_fixtures_from_bq script,
        based on the configuration settings passed in by the user
        """

        print("Preview Mode: Based on the following settings...")
        print(f"State code: {self.state_code}")
        print(
            f"Root entity external id columns: {self.root_entity_external_id_columns}"
        )
        print(f"Ingest view tag: {self.ingest_view_tag}")
        print(f"Output filename: {self.output_filename}")
        print(f"Root entity external ids {self.root_entity_external_ids}")

        print("\nThe script will pull data from the following tables:")
        table_data = []
        warnings = []
        for (
            raw_table_dependency_config
        ) in self.ingest_view_raw_table_dependency_configs:
            root_entity_external_id_columns = [
                column.name
                for column in raw_table_dependency_config.current_columns
                if column.name in self.root_entity_external_id_columns
            ]

            columns_to_randomize = [
                column_info.name
                for column_info in self.columns_to_randomize_by_table[
                    raw_table_dependency_config.raw_file_config.file_tag
                ]
            ]

            table_data.append(
                [
                    raw_table_dependency_config.raw_file_config.file_tag,
                    ", ".join(root_entity_external_id_columns) or "None",
                    ", ".join(columns_to_randomize) or "None",
                ]
            )

            if not root_entity_external_id_columns:
                warnings.append(
                    f"No external ID columns found for table {raw_table_dependency_config.raw_file_config.file_tag}. This table will not be filtered."
                )

        print(
            tabulate(
                table_data,
                headers=[
                    "Raw Table",
                    "Root Entity External ID Columns",
                    "Columns to Randomize",
                ],
                tablefmt="fancy_grid",
            )
        )

        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(warning)

        input_prompt = "\nDo you want to proceed with these settings?"
        prompt_for_confirmation(input_prompt)

    def generate_fixtures_for_ingest_view(self) -> None:
        """Queries BigQuery for the provided raw table and writes the results to CSV."""

        self.preview_generation()

        for (
            raw_table_dependency_config
        ) in self.ingest_view_raw_table_dependency_configs:
            self.validate_raw_table_exists(raw_table_dependency_config.raw_file_config)

            output_fixture_path = fixture_path_for_raw_data_dependency(
                self.state_code,
                raw_table_dependency_config,
                self.output_filename,
            )
            # Don't query code file if it already exists.
            if (
                raw_table_dependency_config.is_code_file
                and os.path.exists(output_fixture_path)
                and not self.overwrite
            ):
                print(
                    f"Fixture already exists for {raw_table_dependency_config.file_tag}. Skipping..."
                )
                continue

            # Get all person external ID columns present in this raw table config
            root_entity_external_id_columns = [
                column.name
                for column in raw_table_dependency_config.current_columns
                if column.name in self.root_entity_external_id_columns
            ]

            raw_table_file_tag = raw_table_dependency_config.file_tag
            if (
                not root_entity_external_id_columns
                and self.file_tags_to_load_in_full
                and raw_table_file_tag not in self.file_tags_to_load_in_full
            ):
                raise ValueError(
                    f"Expected raw table {raw_table_file_tag} to have an ID column to filter "
                    f"by. If this table should be loaded in full, add file_tag in the list param "
                    f"[--file_tags_to_load_in_full]."
                )

            query_str = self.build_query_for_raw_table(
                raw_table_dependency_config=raw_table_dependency_config,
                root_entity_external_id_columns=root_entity_external_id_columns,
            )

            query_job = self.bq_client.run_query_async(
                query_str=query_str, use_query_cache=True
            )
            query_results = query_job.to_dataframe()
            query_results = self.randomize_column_data(query_results)
            write_raw_fixture_dataframe_to_path(query_results, output_fixture_path)
