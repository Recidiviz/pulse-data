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
from typing import Dict, List, Optional, Tuple

import numpy
from faker import Faker
from google.cloud import bigquery
from more_itertools import first
from pandas import DataFrame

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestRawDataTableLatestView,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path
from recidiviz.utils.regions import get_region

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
            randomized_value = FAKE.first_name_nonbinary()
        surname_strs = {"surname", "last", "l", "sur"}
        if any(x in column_info.name for x in surname_strs):
            randomized_value = FAKE.last_name()
    else:
        randomized_value = ""
        for character in value:
            if character.isnumeric():
                randomized_value += str(numpy.random.randint(1, 9, 1)[0])
            if character.isalpha():
                randomized_value += numpy.random.choice(
                    list(string.ascii_uppercase), size=1
                )[0]

    print(f"Randomizing original value {value} to random value {randomized_value}")
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
        person_external_ids: List[str],
        person_external_id_columns: List[str],
        columns_to_randomize: List[str],
        file_tags_to_load_in_full: List[str],
        randomized_values_map: Dict[str, str],
        datetime_format: str,
        overwrite: Optional[bool] = False,
    ):
        self.project_id = project_id
        self.region_code = region_code
        self.ingest_view_tag = ingest_view_tag
        self.output_filename = output_filename
        self.person_external_ids = person_external_ids
        self.person_external_id_columns = person_external_id_columns
        self.file_tags_to_load_in_full = file_tags_to_load_in_full
        self.randomized_values_map = randomized_values_map
        self.datetime_format = datetime_format
        self.overwrite = overwrite

        self.bq_client = BigQueryClientImpl(project_id=project_id)

        view_builder = DirectIngestPreProcessedIngestViewCollector(
            get_region(region_code, is_direct_ingest=True), []
        ).get_view_builder_by_view_name(ingest_view_tag)

        self.ingest_view_raw_table_configs = (
            view_builder.build().raw_table_dependency_configs
        )

        # TODO(#12178) Rely only on pii fields once all states have labeled PII fields.
        self.columns_to_randomize = [
            column
            for raw_config in self.ingest_view_raw_table_configs
            for column in raw_config.columns
            if column.is_pii or column.name in columns_to_randomize
        ]

    def get_output_fixture_path(self, raw_table_file_tag: str) -> str:
        return direct_ingest_fixture_path(
            region_code=self.region_code,
            file_name=f"{self.output_filename}.csv",
            file_tag=raw_table_file_tag,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

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
        table: str,
        person_external_id_columns: List[str],
    ) -> str:

        filter_by_values = ", ".join(
            f"'{id_filter}'" for id_filter in self.person_external_ids
        )

        person_external_id_column = (
            person_external_id_columns.pop(0) if person_external_id_columns else None
        )

        id_filter_condition = (
            f"WHERE {person_external_id_column} IN ({filter_by_values})"
            if person_external_id_column
            else ""
        )

        if person_external_id_columns and id_filter_condition:
            for person_external_id_column in person_external_id_columns:
                id_filter_condition += (
                    f" OR {person_external_id_column} IN ({filter_by_values})"
                )

        query_str = f"SELECT * FROM {table} {id_filter_condition};"
        print(query_str)
        return query_str

    def validate_dateset_and_view_exist(
        self, raw_table_config: DirectIngestRawFileConfig
    ) -> Tuple[DirectIngestRawDataTableLatestView, bigquery.DatasetReference]:
        raw_table_view = DirectIngestRawDataTableLatestView(
            project_id=self.project_id,
            region_code=self.region_code,
            raw_file_config=raw_table_config,
        )
        dataset_ref = bigquery.DatasetReference.from_string(
            raw_table_view.dataset_id, default_project=raw_table_view.project
        )
        if not self.bq_client.table_exists(
            dataset_ref, table_id=raw_table_view.view_id
        ):
            raise Exception(
                f"Table [{raw_table_view.dataset_id}].[{raw_table_view.view_id}] does not exist, exiting."
            )
        return raw_table_view, dataset_ref

    def randomize_column_data(
        self,
        query_results: DataFrame,
    ) -> DataFrame:
        """Given a dataframe and a list of columns to randomize, returns a dataframe with those values randomized. If the
        value already exists in the randomized_values_map, then it uses the existing randomized value. Otherwise it
        generates a random value and saves it in the randomized_values_map for the next raw table in the iteration."""
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
            column_info: RawTableColumnInfo, original_value: str
        ) -> str:
            if original_value not in self.randomized_values_map:
                self.randomized_values_map[original_value] = randomize_value(
                    original_value, column_info, self.datetime_format
                )
            return self.randomized_values_map[original_value]

        for column in columns_to_randomize_for_table:
            distinct_values_to_randomize[
                original_col_name_to_random_col_name[column]
            ] = distinct_values_to_randomize[column].apply(
                lambda val: partial(
                    find_or_create_randomized_value,
                    column_info_for_columns_to_randomize[column],
                )(val)
                if val
                else ""
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
        for raw_table_config in self.ingest_view_raw_table_configs:
            raw_table_file_tag = raw_table_config.file_tag
            raw_table_view, dataset_ref = self.validate_dateset_and_view_exist(
                raw_table_config
            )

            # Write fixtures to this path
            output_fixture_path = self.get_output_fixture_path(raw_table_file_tag)

            # Get all person external ID columns present in this raw table config
            person_external_id_columns = [
                column.name
                for column in raw_table_config.columns
                if column.name in self.person_external_id_columns
            ]

            if (
                not person_external_id_columns
                and self.file_tags_to_load_in_full
                and raw_table_file_tag not in self.file_tags_to_load_in_full
            ):
                raise ValueError(
                    f"Expected raw table {raw_table_file_tag} to have an ID column to filter "
                    f"by. If this table should be loaded in full, add file_tag in the list param "
                    f"[--file_tags_to_load_in_full]."
                )

            query_str = self.build_query_for_raw_table(
                table=f"{self.project_id}.{dataset_ref.dataset_id}.{raw_table_view.view_id}",
                person_external_id_columns=person_external_id_columns,
            )

            query_job = self.bq_client.run_query_async(query_str)
            query_results = query_job.to_dataframe()
            query_results = self.randomize_column_data(query_results)
            self.write_results_to_csv(query_results, output_fixture_path)
