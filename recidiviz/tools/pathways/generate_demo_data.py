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
import random
import sys
import tempfile
from datetime import date, timedelta
from itertools import product
from typing import Dict, List, Optional, Sequence, Set, Tuple

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.persistence.database.schema_utils import get_pathways_table_classes

dimension_possible_values: Dict[str, Sequence] = {
    "age_group": ["<25", "25-29", "30-34", "60+"],
    "facility": ["FACILITY_1", "FACILITY_2", "FACILITY_3", "FACILITY_4", "FACILITY_5"],
    "gender": ["MALE", "FEMALE"],
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
    "time_period": ["months_0_6", "months_7_12", "months_13_24", "months_25_60"],
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
    "60+": range(60, 100),
}

random_value_columns = {
    "full_name": lambda: f"{random.choice(last_names)}, {random.choice(first_names)}",
    "person_id": lambda: str(random.randint(1, 9999999)).zfill(10),
    "state_id": lambda: str(random.randint(1, 99999)).zfill(5),
    "supervising_officer": lambda: str(random.randint(1, 999)).zfill(3),
}


def generate_row(
    year: int, month: int, state_code: str, dimensions: Dict, db_columns: List
) -> Dict:
    # Pick a random valid date for the month, it doesn't really matter what they are
    # so just limit to days 1-28 so we don't have to use logic to determine how many
    # days are in the month we're choosing.
    transition_date = date(year, month, random.randint(1, 28))
    row = {
        "year": year,
        "month": month,
        "transition_date": transition_date,
        "state_code": state_code,
    } | dimensions

    for column, fn in random_value_columns.items():
        if column in db_columns:
            row[column] = fn()

    if "age" in db_columns:
        row["age"] = random.choice(ages_for_age_groups[dimensions["age_group"]])

    if "supervision_start_date" in db_columns:
        row["supervision_start_date"] = transition_date - timedelta(
            days=random.randint(1, 3650)
        )

    return row


def generate_rows(state_code: str, columns: List[str], year: int, month: int) -> List:
    rows = []

    # Figure out which dimensions are applicable for this DB and get all possible permutations
    dimension_values = {
        key: value for key, value in dimension_possible_values.items() if key in columns
    }

    # Store a set of used days/person IDs to avoid duplicate primary keys. The chances of a run of
    # the script encountering a duplicate is surprisingly high due to the birthday problem, and
    # though we could just generate more random bits, this brings the probability to zero instead of
    # low.
    used_keys: Set[Tuple[int, int]] = set()

    for combo in (
        dict(zip(dimension_values, x)) for x in product(*dimension_values.values())
    ):
        # Create between 0 and 2 rows for that dimension
        for _ in range(0, random.randint(0, 2)):
            while True:
                row = generate_row(year, month, state_code, combo, columns)
                # Check if we've found one that hasn't been used yet.
                row_key = (row["transition_date"], row["person_id"])
                if row_key not in used_keys:
                    break
            used_keys.add(row_key)
            rows.append(row)

    return rows


def generate_demo_data(state_code: str, columns: List[str]) -> List:
    """Generates demo data for the new Pathways backend.
    For each year and month over 5 years, we loop through all possible permutations of dimension values.
    For each permutation, we choose a random number of rows to create with those values.
    For non-dimension columns, we pick a realistic random value.
    """
    rows = []
    # Loop over each month from Jan 2017 to Dec 2021
    for year in range(2017, 2022):
        for month in range(1, 13):
            rows += generate_rows(state_code, columns, year, month)

    return rows


def main(
    state_codes: List[str], views: Optional[List[str]], bucket: Optional[str]
) -> None:
    """Generates demo Pathways data for the specified states and views and writes the result to a
    local file or GCS bucket."""
    gcsfs = GcsfsFactory.build()

    tables = (
        [table for table in get_pathways_table_classes() if table.name in views]
        if views
        else get_pathways_table_classes()
    )

    for state_code in state_codes:
        for table in tables:
            logging.info(
                "Generating demo data for state '%s', view '%s'",
                state_code,
                table.name,
            )
            columns = [column.name for column in table.columns]
            rows = generate_demo_data(state_code, columns)
            if bucket:
                with tempfile.NamedTemporaryFile(mode="r+") as f:
                    logging.info("Writing output to temporary file %s", f.name)
                    writer = csv.DictWriter(f, columns)
                    writer.writerows(rows)

                    # Rewind for uploading
                    f.seek(0)
                    object_name = f"{state_code}/{table.name}.csv"
                    logging.info("Uploading to %s/%s", bucket, object_name)
                    gcsfs.upload_from_contents_handle_stream(
                        path=GcsfsFilePath(bucket_name=bucket, blob_name=object_name),
                        contents_handle=LocalFileContentsHandle(
                            local_file_path=f.name, cleanup_file=False
                        ),
                        content_type="text/csv",
                        timeout=300,
                    )
            else:
                with open(f"{state_code}_{table.name}.csv", "w", encoding="utf-8") as f:
                    logging.info("Writing output to %s", f.name)
                    writer = csv.DictWriter(f, columns)
                    writer.writerows(rows)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
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
        choices=[table.name for table in get_pathways_table_classes()],
        nargs="*",
        required=False,
    )

    parser.add_argument(
        "--gcs_bucket",
        help="The bucket to write CSV data to. If empty, writes to a local file.",
        required=False,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)
    main(known_args.state_codes, known_args.views, known_args.gcs_bucket)
