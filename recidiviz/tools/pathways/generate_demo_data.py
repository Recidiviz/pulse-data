# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A script for generating demo data to be used in the new pathways backend.

This can be run with the following command:
    python -m recidiviz.tools.pathways.generate_demo_data \
        --state_codes [US_XX US_YY etc.] \
        --views [VIEW NAMES] (optional)\
        --gcs_bucket [GCS_BUCKET_PATH] \
"""

import argparse
import csv
import logging
import os
import random
import sys
import tempfile
from datetime import date, timedelta
from itertools import product
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union

from dateutil.relativedelta import relativedelta
from sqlalchemy.inspection import inspect

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.schema.pathways.schema import MetricMetadata
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_pathways_database_entities,
)

dimension_possible_values: Dict[str, Sequence] = {
    "admission_reason": ["NEW_ADMISSION", "REVOCATION", "TRANSFER"],
    "age_group": [
        "<25",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60+",
    ],
    "facility": ["FACILITY_1", "FACILITY_2", "FACILITY_3", "FACILITY_4", "FACILITY_5"],
    "gender": ["MALE", "FEMALE"],
    "sex": ["MALE", "FEMALE", "OTHER"],
    "judicial_district": [
        "JUDICIAL_DISTRICT_1",
        "JUDICIAL_DISTRICT_2",
        "JUDICIAL_DISTRICT_3",
        "JUDICIAL_DISTRICT_4",
        "JUDICIAL_DISTRICT_5",
    ],
    "length_of_stay": ["months_3_6", "months_9_12", "months_12_18", "months_18_24"],
    "prior_length_of_incarceration": ["0", "6", "12", "18", "24"],
    "race": ["BLACK", "HISPANIC", "WHITE"],
    "supervision_district": [
        "DISTRICT_1",
        "DISTRICT_2",
        "DISTRICT_3",
        "DISTRICT_4",
        "DISTRICT_5",
    ],
    "supervision_level": ["MINIMUM", "MAXIMUM"],
    "supervision_type": ["PAROLE", "PROBATION"],
}

first_names = [
    "Amelia",
    "Ava",
    "Benjamin",
    "Charlotte",
    "Elijah",
    "Emma",
    "Ethan",
    "Evelyn",
    "Harper",
    "Isabella",
    "James",
    "Liam",
    "Lucas",
    "Mason",
    "Mia",
    "Noah",
    "Oliver",
    "Olivia",
    "Sophia",
    "William",
]

last_names = [
    "Anderson",
    "Brown",
    "Davis",
    "Garcia",
    "Gonzales",
    "Johnson",
    "Jones",
    "Rodriguez",
    "Smith",
    "Williams",
]

ages_for_age_groups = {
    "<25": range(18, 25),
    "25-29": range(25, 30),
    "30-34": range(30, 35),
    "35-39": range(35, 40),
    "40-44": range(40, 45),
    "45-49": range(45, 50),
    "50-54": range(50, 55),
    "55-59": range(55, 60),
    "60+": range(60, 100),
}

random_value_columns = {
    "full_name": lambda: f"{random.choice(last_names)}, {random.choice(first_names)}",
    "person_id": lambda: str(random.randint(1, 9999999)).zfill(10),
    "state_id": lambda: str(random.randint(1, 99999)).zfill(5),
    "supervising_officer": lambda: str(random.randint(1, 999)).zfill(3),
}

# Calculate time periods based on the first day of the last month of available data so we don't end
# up with an empty month at the left side of a chart.
NOW_DATE = date(2021, 12, 1)
LAST_UPDATED_BEGIN_DATE = date(2022, 1, 1)


def get_time_period(relative_date: date) -> str:
    if relative_date >= NOW_DATE - relativedelta(months=6):
        return "months_0_6"
    if relative_date >= NOW_DATE - relativedelta(months=12):
        return "months_7_12"
    if relative_date >= NOW_DATE - relativedelta(months=24):
        return "months_13_24"
    return "months_25_60"


def generate_row(
    state_code: str,
    table_name: str,
    dimensions: Dict,
    db_columns: List,
    month_year: Optional[date] = None,
) -> Dict:
    """Generates a single row with all columns filled out."""
    row: Dict[str, Union[str, int, date]] = {
        "state_code": state_code,
    } | dimensions

    if month_year:
        generated_date = (
            month_year
            if table_name.endswith("_over_time")
            else date(month_year.year, month_year.month, random.randint(1, 28))
        )
        potential_row: Dict[str, Union[str, int, date]] = {
            "year": generated_date.year,
            "month": generated_date.month,
            "transition_date": generated_date,
            "date_in_population": generated_date,
            "supervision_start_date": generated_date
            - timedelta(days=random.randint(1, 3650)),
            "time_period": get_time_period(generated_date),
        }

        row.update(
            {key: value for key, value in potential_row.items() if key in db_columns}
        )

    for column, fn in random_value_columns.items():
        if column in db_columns:
            row[column] = fn()

    if "age" in db_columns:
        row["age"] = random.choice(ages_for_age_groups[dimensions["age_group"]])

    return row


def generate_rows(
    state_code: str,
    table_name: str,
    columns: List[str],
    primary_keys: List[str],
    month_year: Optional[date] = None,
) -> List:
    """Generates rows for this metric in the given month, with 0-2 entries for each permutation
    of dimensions"""
    rows = []

    # Figure out which dimensions are applicable for this DB and get all possible permutations
    dimension_values = {
        key: value for key, value in dimension_possible_values.items() if key in columns
    }

    # Store a set of used primary keys to avoid duplicate primary keys. The chances of a run of
    # the script encountering a duplicate ID is surprisingly high due to the birthday problem, and
    # though we could just generate more random bits, this brings the probability to zero instead of
    # low.
    used_keys: Set[Tuple] = set()

    for combo in (
        dict(zip(dimension_values, x)) for x in product(*dimension_values.values())
    ):
        # Create between 0 and 2 rows for that dimension
        for _ in range(0, random.randint(0, 2)):
            while True:
                row = generate_row(state_code, table_name, combo, columns, month_year)
                # Check if we've found one that hasn't been used yet.
                row_key = tuple(row[key] for key in primary_keys)
                if row_key not in used_keys:
                    break
            used_keys.add(row_key)
            rows.append(row.copy())

    return rows


def generate_demo_data(
    state_code: str, table_name: str, columns: List[str], primary_keys: List[str]
) -> List:
    """Generates demo data for the new Pathways backend.
    For each year and month over 5 years, we loop through all possible permutations of dimension values.
    For each permutation, we choose a random number of rows to create with those values.
    For non-dimension columns, we pick a realistic random value.
    """
    rows = []

    if "transition_date" in columns or "date_in_population" in columns:
        # Loop over each month from Jan 2017 to Dec 2021
        for year in range(2017, 2022):
            for month in range(1, 13):
                # the actual day of the month will be redefined later
                rows += generate_rows(
                    state_code, table_name, columns, primary_keys, date(year, month, 1)
                )
    else:
        rows = generate_rows(state_code, table_name, columns, primary_keys)

    return rows


def generate_demo_metric_metadata(tables: List[SQLAlchemyModelType]) -> List:
    last_updated_date = LAST_UPDATED_BEGIN_DATE
    rows = []
    for table in tables:
        if table.get_entity_name() == "metric_metadata":
            continue
        rows.append({"metric": table.__name__, "last_updated": last_updated_date})
        last_updated_date += timedelta(days=1)
    return rows


def main(
    state_codes: List[str],
    views: Optional[List[str]],
    bucket: Optional[str],
    headers: Optional[bool],
    output_directory: Optional[str],
) -> None:
    """Generates demo Pathways data for the specified states and views and writes the result to a
    local file or GCS bucket."""

    tables = (
        [
            table
            for table in get_pathways_database_entities()
            if table.get_entity_name() in views
        ]
        if views
        else [
            table
            for table in get_pathways_database_entities()
            if not table.get_entity_name().endswith("projection")
            and not table.get_entity_name().endswith("impact")
        ]
    )
    metric_metadata = generate_demo_metric_metadata(tables)

    for state_code in state_codes:
        for table in tables:
            table_name = table.get_entity_name()
            logging.info(
                "Generating demo data for state '%s', view '%s'",
                state_code,
                table_name,
            )
            columns = [column.name for column in inspect(table).c]
            primary_keys = [column.name for column in inspect(table).primary_key]
            rows = (
                metric_metadata
                if table == MetricMetadata
                else generate_demo_data(state_code, table_name, columns, primary_keys)
            )
            if bucket:
                gcsfs = GcsfsFactory.build()
                metadata = next(
                    (
                        {"last_updated": str(m["last_updated"])}
                        for m in metric_metadata
                        if m["metric"] == table.__name__
                    ),
                    None,
                )
                with tempfile.NamedTemporaryFile(mode="r+") as f:
                    logging.info("Writing output to temporary file %s", f.name)
                    writer = csv.DictWriter(f, columns)
                    if headers:
                        writer.writeheader()
                    writer.writerows(rows)

                    # Rewind for uploading
                    f.seek(0)
                    object_name = f"{state_code}/{table_name}.csv"
                    logging.info("Uploading to %s/%s", bucket, object_name)
                    gcsfs.upload_from_contents_handle_stream(
                        path=GcsfsFilePath(bucket_name=bucket, blob_name=object_name),
                        contents_handle=LocalFileContentsHandle(
                            local_file_path=f.name, cleanup_file=False
                        ),
                        content_type="text/csv",
                        timeout=300,
                        metadata=metadata,
                    )
            else:
                filename = (
                    f"{table_name}.csv"
                    if len(state_codes) == 1
                    else f"{state_code}_{table_name}.csv"
                )
                with open(
                    os.path.join(output_directory or ".", filename),
                    "w",
                    encoding="utf-8",
                ) as f:
                    logging.info("Writing output to %s", f.name)
                    writer = csv.DictWriter(f, columns)
                    if headers:
                        writer.writeheader()
                    writer.writerows(rows)


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state_codes",
        help="State codes to generate demo data for.",
        type=str,
        nargs="+",
        required=True,
    )

    parser.add_argument(
        "--views",
        help="Space-separated views to generate demo data for. If empty, generates for all pathways event-level views.",
        type=str,
        choices=[
            table.name
            for table in get_all_table_classes_in_schema(SchemaType.PATHWAYS)
            if not table.name.endswith("projection")
            and not table.name.endswith("impact")
        ],
        nargs="*",
        required=False,
    )

    parser.add_argument(
        "--gcs_bucket",
        help="The bucket to write CSV data to. If empty, writes to a local file.",
        required=False,
    )

    parser.add_argument(
        "--headers",
        help="Whether to write headers into the CSV file. Defaults to True. Specify --no-headers to set to false.",
        action=argparse.BooleanOptionalAction,
        default=True,
        required=False,
    )

    parser.add_argument(
        "--output_directory",
        help="""The local directory to write files to. If empty and gcs_bucket is not set to True,
        writes to the directory the script is run from.""",
        required=False,
    )

    known_args, _ = parser.parse_known_args(argv)
    if known_args.gcs_bucket and known_args.output_directory:
        parser.error("Cannot set both --gcs_bucket and --output_directory")
    return known_args


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)
    main(
        args.state_codes,
        args.views,
        args.gcs_bucket,
        args.headers,
        args.output_directory,
    )
