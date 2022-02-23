#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
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
"""Tool to generate raw data fixture files to test ingest view queries. It generates a CSV fixture for each raw data
table referenced in the ingest view query. The script requires a list of person-level IDs to filter by and a list
of columns to use to filter by person ID. It optionally takes a list of tables that should be generated in full.
The script also takes a list of columns with data that should be randomized. The script will produce the same
randomized value for matching values across each of the raw data fixtures.

Example Usage:
    python -m recidiviz.tools.ingest.testing.generate_raw_data_fixtures_from_bq --region_code US_ME \
    --ingest_view_tag CLIENT \
    --output_filename basic \
    --columns_to_randomize CIS_100_CLIENT_ID \
    --person_external_ids 111,222,333
    --person_external_id_columns Cis_100_Client_Id Cis_Client_Id \
    [--file_tags_to_load_in_full CIS_3150_TRANSFER_TYPE OTHER_TABLE] \
    [--project_id GCP_PROJECT_STAGING] \
    [--overwrite True] \
    [--randomized_values_map '{"123": "456"}'] \
"""
import argparse
import json
import os
import string
import sys
from typing import Dict, List, Optional

import numpy
from google.cloud import bigquery
from more_itertools import only
from pandas import DataFrame

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants import states
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestRawDataTableLatestView,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.regions import get_region

RANDOMIZED_COLUMN_PREFIX = "recidiviz_randomized_"


def randomize_value(value: str) -> str:
    """For each character in a string, checks whether the character is a number or letter and replaces it with a random
    matching type character."""
    randomized_value = ""
    for character in value:
        if character.isnumeric():
            randomized_value += str(numpy.random.randint(0, 9, 1)[0])
        if character.isalpha():
            randomized_value += numpy.random.choice(
                list(string.ascii_uppercase), size=1
            )[0]

    print(f"Randomizing original value {value} to random value {randomized_value}")
    return randomized_value


def randomize_column_data(
    query_results: DataFrame,
    columns_to_randomize: List[str],
    randomized_values_map: Dict[str, str],
) -> DataFrame:
    """Given a dataframe and a list of columns to randomize, returns a dataframe with those values randomized. If the
    value already exists in the randomized_values_map, then it uses the existing randomized value. Otherwise it
    generates a random value and saves it in the randomized_values_map for the next raw table in the iteration."""
    original_column_order = list(query_results.columns)

    if not columns_to_randomize:
        return query_results

    columns_to_randomize_for_table = [
        column for column in original_column_order if column in columns_to_randomize
    ]

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

    def find_or_create_randomized_value(original_value: str) -> str:
        if original_value not in randomized_values_map:
            randomized_values_map[original_value] = randomize_value(original_value)
        return randomized_values_map[original_value]

    for column in columns_to_randomize_for_table:
        distinct_values_to_randomize[
            original_col_name_to_random_col_name[column]
        ] = distinct_values_to_randomize[column].apply(
            lambda val: find_or_create_randomized_value(val) if val else ""
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


def build_query_for_raw_table(
    table: str,
    person_external_ids: List[str],
    person_external_id_column: Optional[str],
) -> str:

    filter_by_values = ", ".join(f"'{id_filter}'" for id_filter in person_external_ids)

    id_filter_condition = (
        f"WHERE {person_external_id_column} IN ({filter_by_values})"
        if person_external_id_column
        else ""
    )
    query_str = f"SELECT * FROM {table} {id_filter_condition};"
    print(query_str)
    return query_str


def write_results_to_csv(
    query_results: DataFrame, output_fixture_path: str, overwrite: bool
) -> None:
    os.makedirs(os.path.dirname(output_fixture_path), exist_ok=True)
    include_headers = overwrite or not os.path.exists(output_fixture_path)
    write_disposition = "w" if overwrite else "a"

    query_results.sort_values(by=list(query_results.columns))
    query_results.to_csv(
        output_fixture_path,
        mode=write_disposition,
        header=include_headers,
        index=False,
    )


def output_fixture_for_raw_file(
    bq_client: BigQueryClientImpl,
    raw_table_config: DirectIngestRawFileConfig,
    raw_table: str,
    person_external_ids: List[str],
    person_external_id_columns: List[str],
    columns_to_randomize: List[str],
    file_tags_to_load_in_full: List[str],
    randomized_values_map: Dict[str, str],
    output_fixture_path: str,
    overwrite: bool,
) -> None:
    """Queries BigQuery for the provided raw table and writes the results to CSV."""
    raw_table_file_tag = raw_table_config.file_tag

    person_external_id_column = only(
        [
            column.name
            for column in raw_table_config.columns
            if column.name in person_external_id_columns
        ]
    )

    if (
        not person_external_id_column
        and file_tags_to_load_in_full
        and raw_table_file_tag not in file_tags_to_load_in_full
    ):
        raise ValueError(
            f"Expected raw table {raw_table_file_tag} to have an ID column to filter "
            f"by. If this table should be loaded in full, add file_tag in the list param "
            f"[--file_tags_to_load_in_full]."
        )

    query_str = build_query_for_raw_table(
        table=raw_table,
        person_external_ids=person_external_ids,
        person_external_id_column=person_external_id_column,
    )

    query_job = bq_client.run_query_async(query_str)
    query_results = query_job.to_dataframe()
    query_results = randomize_column_data(
        query_results, columns_to_randomize, randomized_values_map
    )
    write_results_to_csv(query_results, output_fixture_path, overwrite)


def main(
    project_id: str,
    region_code: str,
    ingest_view_tag: str,
    output_filename: str,
    person_external_ids: List[str],
    person_external_id_columns: List[str],
    columns_to_randomize: List[str],
    file_tags_to_load_in_full: List[str],
    randomized_values_map: Dict[str, str],
    overwrite: bool = False,
) -> None:
    """Generate raw data fixtures from the latest views in BQ to be used as mock data in view query tests."""
    bq_client = BigQueryClientImpl(project_id=project_id)

    view_builder = DirectIngestPreProcessedIngestViewCollector(
        get_region(region_code, is_direct_ingest=True), []
    ).get_view_builder_by_file_tag(ingest_view_tag)

    ingest_view_raw_table_configs = view_builder.build().raw_table_dependency_configs

    for raw_table_config in ingest_view_raw_table_configs:
        raw_table_view = DirectIngestRawDataTableLatestView(
            project_id=project_id,
            region_code=region_code,
            raw_file_config=raw_table_config,
        )
        dataset_ref = bigquery.DatasetReference.from_string(
            raw_table_view.dataset_id, default_project=raw_table_view.project
        )
        if not bq_client.table_exists(dataset_ref, table_id=raw_table_view.view_id):
            raise Exception(
                f"Table [{raw_table_view.dataset_id}].[{raw_table_view.view_id}] does not exist, exiting."
            )
        output_fixture_path = direct_ingest_fixture_path(
            region_code=region_code,
            file_name=f"{output_filename}.csv",
            file_tag=raw_table_config.file_tag,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        output_fixture_for_raw_file(
            bq_client=bq_client,
            raw_table_config=raw_table_config,
            raw_table=f"{project_id}.{dataset_ref.dataset_id}.{raw_table_view.view_id}",
            person_external_ids=person_external_ids,
            person_external_id_columns=person_external_id_columns,
            columns_to_randomize=columns_to_randomize,
            file_tags_to_load_in_full=file_tags_to_load_in_full,
            randomized_values_map=randomized_values_map,
            output_fixture_path=output_fixture_path,
            overwrite=overwrite,
        )


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the named arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        required=False,
    )

    parser.add_argument(
        "--region_code",
        dest="region_code",
        help="The region code to query data for in the form US_XX.",
        type=str,
        choices=[state.value for state in states.StateCode],
        required=True,
    )

    parser.add_argument(
        "--ingest_view_tag",
        dest="ingest_view_tag",
        help="The ingest view file_tag that this script is generating fixtures for.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--output_filename",
        dest="output_filename",
        help="Filename for all raw file fixtures generated by this script run, to distinguish them from other fixture "
        "data for the same table.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--columns_to_randomize",
        dest="columns_to_randomize",
        help="Column names that have alphanumeric ID-like values to randomize.",
        nargs="+",
        default=[],
        required=True,
    )

    parser.add_argument(
        "--person_external_ids",
        dest="person_external_ids",
        help="Person-level IDs to filter the dataset by.",
        nargs="+",
        default=[],
        required=True,
    )

    parser.add_argument(
        "--person_external_id_columns",
        dest="person_external_id_columns",
        help="Column names that map to the person-level external IDs.",
        nargs="+",
        default=[],
        required=True,
    )

    parser.add_argument(
        "--file_tags_to_load_in_full",
        dest="file_tags_to_load_in_full",
        help="Raw table names that should be loaded fully and not filtered by an ID field.",
        required=False,
        nargs="+",
        default=[],
    )

    parser.add_argument(
        "--overwrite",
        dest="overwrite",
        help="Whether or not to overwrite any existing raw data fixtures. Default is False and this will append "
        "data to the existing raw data fixture files.",
        required=False,
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "--randomized_values_map",
        dest="randomized_values_map",
        help="A mapping of a person_external_id to a randomized value to export it as. This is useful for updating "
        "existing fixtures with known person_external_id random values.",
        required=False,
        default={},
        type=json.loads,
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    args = parse_arguments(sys.argv)

    with local_project_id_override(GCP_PROJECT_STAGING):
        main(
            project_id=args.project_id,
            region_code=args.region_code,
            ingest_view_tag=args.ingest_view_tag,
            output_filename=args.output_filename,
            person_external_ids=args.person_external_ids,
            person_external_id_columns=args.person_external_id_columns,
            columns_to_randomize=args.columns_to_randomize,
            file_tags_to_load_in_full=args.file_tags_to_load_in_full,
            randomized_values_map=args.randomized_values_map,
            overwrite=args.overwrite,
        )
