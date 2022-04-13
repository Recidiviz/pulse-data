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
"""Tool to re-generate raw data fixtures for ingest view tests.

Use this script if there's been a change to the raw data inputs for an ingest view
that already has fixtures generated for its tests. This script requires a
a Google Sheet ID for a google sheet that has the following schema:

- person_external_id: The original person external ID the fixture was generated for
- test_id: The original test ID that was generated for this person external ID
- output_filename: The fixture filename that is associated with this person_external_id
- ingest_view_tag: The ingest view name that this test fixture was generated for

You also need the Google API credentials.json file downloaded and saved locally. Pass in the
local path to the credentials directory to the --credentials_directory path. This utility is
expecting the path to be: `/Your/local/path/credentials.json`. For more instruction on downloading
the credentials file, see the recidiviz.utils.google_drive utility.

YOU MUST LIST ALL PII COLUMNS FOR THE `columns_to_randomize` OPTION. Not just identifiers, but anything that is
personally identifiable should be randomized as part of usage of this script. If you are unsure as to what constitutes
PII, please reach out to a teammate to discuss.

Example Usage:
    python -m recidiviz.tools.ingest.testing.regenerate_raw_data_fixtures_from_sheet --region_code US_XX \
    --columns_to_randomize Person_Id_Column First_Name Last_Name Birthdate\
    --person_external_id_columns Person_Id_Column Other_Person_Id_Col \
    --regenerate_fixtures_from_sheet_id GOOGLE_SHEETS_ID \
    --credentials_directory /Your/local/google/credentials/path \

"""
import argparse
import sys
from collections import defaultdict
from typing import Dict, List

from pandas import DataFrame

from recidiviz.common.constants import states
from recidiviz.tools.ingest.testing.raw_data_fixtures_generator import (
    RawDataFixturesGenerator,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.google_drive import get_credentials, get_sheets_service
from recidiviz.utils.metadata import local_project_id_override


def get_values_by_ingest_view_and_output_filename(
    values_df: DataFrame, ingest_view_tag: str
) -> Dict[str, Dict]:
    values_by_ingest_view_and_output_filename: Dict[str, Dict] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(str))
    )
    for _, row in values_df.iterrows():
        if ingest_view_tag and row.ingest_view_tag != ingest_view_tag:
            # If a specific ingest_view_tag was provided, only regenerate fixtures for that ingest view.
            continue
        values_by_ingest_view_and_output_filename[row.ingest_view_tag][
            row.output_filename
        ][row.person_external_id] = row.test_id
    return values_by_ingest_view_and_output_filename


def main(
    project_id: str,
    region_code: str,
    google_sheet_values: List[List[str]],
    ingest_view_tag: str,
    person_external_id_columns: List[str],
    columns_to_randomize: List[str],
) -> None:
    """Re-generate raw data fixtures from the latest views in BQ using a Google Sheet mapping of the previously
    generated test fixture IDs."""

    if not google_sheet_values:
        raise ValueError("No values found in the provided Google Sheet.")

    required_headers = [
        "person_external_id",
        "test_id",
        "output_filename",
        "ingest_view_tag",
    ]
    headers = google_sheet_values.pop(0) if google_sheet_values else None
    for header in required_headers:
        if not headers or header not in headers:
            raise ValueError(
                f"Google Sheet header row missing or missing required header: {header}"
            )

    values_df = DataFrame(google_sheet_values, columns=headers)
    values_by_ingest_view_and_output_filename = (
        get_values_by_ingest_view_and_output_filename(values_df, ingest_view_tag)
    )
    for (
        ingest_view_tag_from_sheet,
        output_filename_values,
    ) in values_by_ingest_view_and_output_filename.items():

        for (
            output_filename,
            external_id_to_test_id_map,
        ) in output_filename_values.items():
            fixtures_generator = RawDataFixturesGenerator(
                project_id=project_id,
                region_code=region_code,
                ingest_view_tag=ingest_view_tag_from_sheet,
                output_filename=output_filename,
                person_external_ids=external_id_to_test_id_map.keys(),
                person_external_id_columns=person_external_id_columns,
                columns_to_randomize=columns_to_randomize,
                file_tags_to_load_in_full=[],
                randomized_values_map=external_id_to_test_id_map,
                datetime_format="%m/%d/%y",
                overwrite=True,
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
        "--regenerate_fixtures_from_sheet_id",
        dest="google_sheet_id",
        help="The Google Sheets ID where the person_external_id is mapped to the previously generated test ID. The "
        "schema for this sheet must include these columns: person_external_id, test_id, output_filename, "
        "ingest_view_tag. You must also pass in --columns_to_randomize. If you only want to regenerate"
        "for a specific ingest_view_tag pass in that option.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--credentials_directory",
        required=True,
        type=str,
        help="Directory where the 'credentials.json' live, as well as the cached token. See recidiviz.utils.google_drive"
        "for instructions on downloading and storing credentials.",
    )

    parser.add_argument(
        "--ingest_view_tag",
        dest="ingest_view_tag",
        help="The ingest view file_tag that this script is regenerating fixtures for.",
        type=str,
        required=False,
    )

    parser.add_argument(
        "--columns_to_randomize",
        dest="columns_to_randomize",
        help="Column names that have alphanumeric PII values to randomize.",
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

    known_args, _ = parser.parse_known_args(argv)

    return known_args


def get_values_from_sheets(
    google_sheet_id: str, credentials_directory: str
) -> List[List[str]]:
    sheets_scope = "https://www.googleapis.com/auth/spreadsheets"
    credentials = get_credentials(
        directory=credentials_directory,
        readonly=True,
        scopes=[sheets_scope],
    )
    sheets_service = get_sheets_service(creds=credentials)
    sheet_request = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=google_sheet_id, range="Sheet1")
        .execute()
    )
    return sheet_request.get("values", [])


if __name__ == "__main__":
    args = parse_arguments(sys.argv)

    with local_project_id_override(GCP_PROJECT_STAGING):
        sheet_values = get_values_from_sheets(
            google_sheet_id=args.google_sheet_id,
            credentials_directory=args.credentials_directory,
        )

        main(
            project_id=args.project_id,
            region_code=args.region_code,
            google_sheet_values=sheet_values,
            ingest_view_tag=args.ingest_view_tag,
            person_external_id_columns=args.person_external_id_columns,
            columns_to_randomize=args.columns_to_randomize,
        )
