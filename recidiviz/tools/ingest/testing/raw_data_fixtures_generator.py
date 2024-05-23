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
import string
from functools import partial
from typing import Dict, List, Optional

import numpy
from faker import Faker
from google.cloud import bigquery
from more_itertools import first
from pandas import DataFrame

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
from recidiviz.tests.ingest.direct.fixture_util import DirectIngestTestFixturePath

Faker.seed(0)
FAKE = Faker(locale=["en-US"])


def randomize_value(
    value: str, column_info: RawTableColumnInfo, datetime_format: str
) -> str:
    """For each character in a string, checks whether the character is a number or letter and replaces it with a random
    matching type character."""
    # TODO(#12179) Improve and unit test randomization options by specifying pii_type.
    randomized_value = ""
    if column_info.is_datetime:
        randomized_value = FAKE.date(pattern=datetime_format)
    elif "name" in column_info.name.lower():
        first_middle_name_strs = {"first", "f", "middle", "m"}
        if any(x in column_info.name for x in first_middle_name_strs):
            randomized_value = FAKE.first_name_nonbinary() + str(FAKE.random_number(2))
        surname_strs = {"surname", "last", "l", "sur"}
        if any(x in column_info.name for x in surname_strs):
            randomized_value = FAKE.last_name() + str(FAKE.random_number(2))
    else:
        randomized_value = ""
        for character in value:
            if character.isnumeric():
                randomized_value += str(numpy.random.randint(1, 9, 1)[0])
            if character.isalpha():
                randomized_value += numpy.random.choice(
                    list(string.ascii_uppercase), size=1
                )[0]

    print(
        f"Randomizing value from column '{column_info.name}': {value} -> {randomized_value}"
    )
    return randomized_value


RANDOMIZED_COLUMN_PREFIX = "recidiviz_randomized_"


class RawDataFixturesGenerator:
    """Class for generating raw data fixtures for ingest view tests."""

    def __init__(
        self,
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
        # TODO(#29995) Show the tables, columns, and filters if True
        preview: bool = False,
        # TODO(#29994) Add functionality to get columns via a regex.
        root_entity_external_id_column_regex: Optional[str] = None,
    ):
        self.project_id = project_id
        self.region_code = region_code
        self.ingest_view_tag = ingest_view_tag
        self.output_filename = output_filename
        self.root_entity_external_ids = root_entity_external_ids
        self.root_entity_external_id_columns = root_entity_external_id_columns
        self.root_entity_external_id_column_regex = root_entity_external_id_column_regex
        self.file_tags_to_load_in_full = file_tags_to_load_in_full
        self.randomized_values_map = randomized_values_map
        self.datetime_format = datetime_format
        self.overwrite = overwrite
        self.preview = preview

        self.bq_client = BigQueryClientImpl(project_id=project_id)

        query_builder = DirectIngestViewQueryBuilderCollector(
            get_direct_ingest_region(region_code), []
        ).get_query_builder_by_view_name(ingest_view_tag)

        self.ingest_view_raw_table_dependency_configs = (
            query_builder.raw_table_dependency_configs
        )

        # TODO(#12178) Rely only on pii fields once all states have labeled PII fields.
        self.columns_to_randomize = [
            column
            for raw_config in self.ingest_view_raw_table_dependency_configs
            for column in raw_config.columns
            if column.is_pii or column.name in columns_to_randomize
        ]
        # Only look at primary instance to find the raw data
        self.raw_data_source_instance = DirectIngestInstance.PRIMARY
        self.query_builder = RawTableQueryBuilder(
            project_id=self.project_id,
            region_code=self.region_code,
            raw_data_source_instance=self.raw_data_source_instance,
        )

    # TODO(#29997) Update the path for code files.
    def get_output_fixture_path(
        self, raw_file_dependency_config: DirectIngestViewRawFileDependency
    ) -> str:
        return DirectIngestTestFixturePath.for_raw_file_fixture(
            region_code=self.region_code,
            file_name=f"{self.output_filename}.csv",
            raw_file_dependency_config=raw_file_dependency_config,
        ).full_path()

    def write_results_to_csv(
        self, query_results: DataFrame, output_fixture_path: str
    ) -> None:
        os.makedirs(os.path.dirname(output_fixture_path), exist_ok=True)
        include_headers = self.overwrite or not os.path.exists(output_fixture_path)
        write_disposition = "w" if self.overwrite else "a"

        query_results.sort_values(by=list(query_results.columns))
        query_results.to_csv(
            output_fixture_path,
            mode=write_disposition,
            header=include_headers,
            index=False,
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
            address_overrides=None,
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
            state_code=StateCode(self.region_code.upper()),
            instance=self.raw_data_source_instance,
            sandbox_dataset_prefix=None,
        )
        dataset_ref = bigquery.DatasetReference.from_string(
            raw_table_dataset_id, default_project=self.project_id
        )
        if not self.bq_client.table_exists(
            dataset_ref, table_id=raw_file_config.file_tag
        ):
            raise RuntimeError(
                f"Table [{raw_table_dataset_id}].[{raw_file_config.file_tag}] does not exist, exiting."
            )

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

    def generate_fixtures_for_ingest_view(self) -> None:
        """Queries BigQuery for the provided raw table and writes the results to CSV."""
        for (
            raw_table_dependency_config
        ) in self.ingest_view_raw_table_dependency_configs:
            self.validate_raw_table_exists(raw_table_dependency_config.raw_file_config)

            # Write fixtures to this path
            # TODO(#29997) Update the path for code files.
            output_fixture_path = self.get_output_fixture_path(
                raw_table_dependency_config
            )

            # Get all person external ID columns present in this raw table config
            root_entity_external_id_columns = [
                column.name
                for column in raw_table_dependency_config.columns
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

            # TODO(#29997) Don't query code file if it already exists.
            query_job = self.bq_client.run_query_async(
                query_str=query_str, use_query_cache=True
            )
            query_results = query_job.to_dataframe()
            query_results = self.randomize_column_data(query_results)
            self.write_results_to_csv(query_results, output_fixture_path)
