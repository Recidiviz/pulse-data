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
"""
Script to retrieve and upload data for New Mexico Administrative Office of the Courts
for the previous month/year combination. This script is intended to run once per month.
Data is retrieved from the dataXchange API. If a given job fails, we can also pass in
month and year as arguments.

python -m recidiviz.justice_counts.jobs.new_mexico_dataxchange \
    --year=2023 \
    --month=1 \
    --dry-run=true
"""

import argparse
import datetime
import logging
from typing import Dict, List

import pandas as pd
import requests
import sentry_sdk
from google.cloud import storage

from recidiviz.justice_counts.utils.constants import (
    JUSTICE_COUNTS_SENTRY_DSN,
    NEW_MEXICO_SUPERAGENCY_BUCKET_PROD,
    NEW_MEXICO_SUPERAGENCY_BUCKET_STAGING,
)
from recidiviz.utils.environment import in_gcp_production
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.secrets import get_secret

METRIC_TO_URL = {
    "cases_filed_by_severity": "https://www.nmdataxchange.gov/resource/wa8m-ubx9.json"
}
logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year",
        help="The year in which you want to retrieve data for.",
        required=False,
        default=datetime.datetime.now(tz=datetime.timezone.utc).year
        if datetime.datetime.now(tz=datetime.timezone.utc).month > 1
        else datetime.datetime.now(tz=datetime.timezone.utc).year - 1,
        type=int,
    )
    parser.add_argument(
        "--month",
        help="The month in which you want to retrieve data for.",
        required=False,
        default=datetime.datetime.now(tz=datetime.timezone.utc).month - 1
        if datetime.datetime.now(tz=datetime.timezone.utc).month > 1
        else 12,
        type=int,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def get_new_mexico_courts_data(
    year: int, month: int, url: str, metric: str, dry_run: bool
) -> None:
    """
    This function retrieves New Mexico data via the dataXchange API for a given year and month.
    It does so by paging through the data, and fetching 10,000 rows per
    API call until all rows are fetched. We then do some manipulation to the data
    to get it in the expected format and then upload it to Publisher via Automated
    Bulk Upload.
    """
    user = get_secret(secret_id="justice_counts_dataXchange_user")  # nosec
    password = get_secret(secret_id="justice_counts_dataXchange_password")  # nosec

    if user is None:
        raise ValueError("Missing required user for New Mexico dataXchange API")

    if password is None:
        raise ValueError("Missing required password for New Mexico dataXchange API")

    all_data: List = []
    continue_fetching = True
    offset = 0
    # Page through the data
    # Each call to the API will fetch 10,000 rows
    # 'offset' is the index of the result array where to start the returned list of results
    while continue_fetching is True:
        page_url = (
            url + f"?year={year}&month={month}&$limit=10000&$offset={offset}&$order=:id"
        )
        response = requests.get(page_url, auth=(user, password), timeout=30)
        if response.status_code != 200:
            # TODO(#23412) Integrate Sentry
            logging.exception(
                "NM DataXchange API call to %s URL failed with %s status code. Response JSON: %s",
                page_url,
                response.status_code,
                response.json(),
            )
            break
        json = response.json()
        all_data.extend(json)
        logging.info("Current offset is: %s", offset)
        offset += 10000
        continue_fetching = len(json) == 10000

    logging.info("Retrieved %s rows from dataXchange API", len(all_data))

    if dry_run is False:
        filename = convert_data_to_xlsx(
            data=all_data, metric=metric, year=year, month=month
        )
        upload_file(filename=filename)


def convert_data_to_xlsx(data: List[Dict], metric: str, year: int, month: int) -> str:
    df = pd.DataFrame.from_records(data=data)
    current_datetime = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    filename = f"courts_{metric}_{year}_{month}_{current_datetime}.xlsx"
    df.to_excel(filename, sheet_name=metric, index=False)
    return filename


def upload_file(filename: str) -> None:
    storage_client = storage.Client()
    if in_gcp_production():
        bucket = storage_client.bucket(NEW_MEXICO_SUPERAGENCY_BUCKET_PROD)
    else:
        bucket = storage_client.bucket(NEW_MEXICO_SUPERAGENCY_BUCKET_STAGING)

    blob = bucket.blob(filename)
    # This will upload the xls file to the appropriate bucket in GCP.
    # Once the file is uploaded to the bucket, a notification will be sent
    # to the pub/sub topic, which will hit the /spreadsheets post endpoint
    # (the file will be ingested via Automated Bulk Upload).
    blob.upload_from_filename(filename)
    logging.info("%s stored in GCP", filename)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )
    args = create_parser().parse_args()
    for global_metric, global_url in METRIC_TO_URL.items():
        get_new_mexico_courts_data(
            year=args.year,
            month=args.month,
            url=global_url,
            metric=global_metric,
            dry_run=args.dry_run,
        )
        logging.info("%s, %s called", args.month, args.year)
