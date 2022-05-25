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
"""Tool to generate raw data fixture files to test ingest view queries.

This script will query BigQuery for each raw table defined in the ingest view and filter on the person external IDs
provided. It can take multiple person external IDs and multiple person external ID column names. It can filter
a table by one or more column names provided, which is useful if there is a table with multiple external ID columns,
like US_PA. The script optionally takes a list of tables that should be generated in full, e.g. tables in the raw data
that exist only to list possible code values for another table, like in US_ME.

The output will be CSV fixtures for each raw data table referenced in the ingest view, with randomized
values for the columns listed in the `columns_to_randomize` option. The script will produce the same
randomized value for matching values across each of the raw data fixtures.

YOU MUST LIST ALL PII COLUMNS FOR THIS `columns_to_randomize` OPTION. Not just identifiers, but anything that is
personally identifiable should be randomized as part of usage of this script. If you are unsure as to what constitutes
PII, please reach out to a teammate to discuss.

The optional `randomized_values_map` can be used to explicitly define what you want specific values found in one of the
`columns_to_randomize` to be transformed into. Any value found in one of the `columns_to_randomize` which is not listed
in `randomized_values_map` will be given a random value, though for non-id columns, it will not necessarily be readable.
This makes `randomized_values_map` especially useful for non-id columns, like birthdays.

Example Usage:
    python -m recidiviz.tools.ingest.testing.generate_raw_data_fixtures_from_bq --region_code US_XX \
    --columns_to_randomize Person_Id_Column First_Name Last_Name Birthdate\
    --person_external_id_columns Person_Id_Column Other_Person_Id_Col \
    --ingest_view_tag incarceration_periods \
    --output_filename basic \
    --person_external_ids 111 222 333 \
    [--file_tags_to_load_in_full CIS_3150_TRANSFER_TYPE OTHER_TABLE] \
    [--project_id GCP_PROJECT_STAGING] \
    [--datetime_format '%m/%d/%y'] \
    [--overwrite True] \
    [--randomized_values_map '{"person_id_here": "randomized_id_here", "birthdate_value_here": "randomized_date_here"}']
"""
import argparse
import json
import sys
from typing import Dict, List

from recidiviz.common.constants import states
from recidiviz.tools.ingest.testing.raw_data_fixtures_generator import (
    RawDataFixturesGenerator,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


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
    datetime_format: str,
    overwrite: bool = False,
) -> None:
    """Generate raw data fixtures from the latest views in BQ to be used as mock data in view query tests."""
    fixtures_generator = RawDataFixturesGenerator(
        project_id=project_id,
        region_code=region_code,
        ingest_view_tag=ingest_view_tag,
        output_filename=output_filename,
        person_external_ids=person_external_ids,
        person_external_id_columns=person_external_id_columns,
        # TODO(#12178) Remove this option once all states have labeled PII fields.
        columns_to_randomize=columns_to_randomize,
        file_tags_to_load_in_full=file_tags_to_load_in_full,
        randomized_values_map=randomized_values_map,
        datetime_format=datetime_format,
        overwrite=overwrite,
    )
    fixtures_generator.generate_fixtures_for_ingest_view()


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
        help="The ingest view that this script is generating fixtures for.",
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

    # TODO(#12178) Remove this option once all states have labeled PII fields.
    parser.add_argument(
        "--columns_to_randomize",
        dest="columns_to_randomize",
        help="Column names that have alphanumeric PII values to randomize.",
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
        "--datetime_format",
        dest="datetime_format",
        help="A datetime format string to specify for any datetime fields in the raw data fixtures that need to be randomized. Default is %m/%d/%y",
        required=False,
        default="%m/%d/%y",
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
            datetime_format=args.datetime_format,
            overwrite=args.overwrite,
        )
