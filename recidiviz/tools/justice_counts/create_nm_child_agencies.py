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
Script to create all child agencies for New Mexico Administrative Office of the Courts.
Agencies are retrieved from the dataXchange API.

python -m recidiviz.tools.justice_counts.create_nm_child_agencies \
    --project-id=PROJECT_ID \
    --dry-run=true
"""


import argparse
import logging

import pandas as pd
import requests

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.secrets import get_secret

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def get_child_agencies(dry_run: bool, project_id: str) -> None:
    user = get_secret(secret_id="justice_counts_dataXchange_user")  # nosec
    password = get_secret(secret_id="justice_counts_dataXchange_password")  # nosec

    if user is None:
        raise ValueError("Missing required user for New Mexico dataXchange API")

    if password is None:
        raise ValueError("Missing required password for New Mexico dataXchange API")

    all_agencies: set = set()
    continue_fetching = True
    offset = 0
    # Page through the data
    # Each call to the API will fetch 10,000 rows
    # 'offset' is the index of the result array where to start the returned list of results
    while continue_fetching:
        url = f"https://www.nmdataxchange.gov/resource/wa8m-ubx9.json?$limit=10000&$offset={offset}&$order=:id"
        response = requests.get(url, auth=(user, password), timeout=30)
        json = response.json()
        df = pd.DataFrame.from_records(data=json)
        if "agency" in df.columns:
            agency_names = set(df["agency"])
            all_agencies.update(agency_names)
        offset += 9999
        logging.info(offset)
        continue_fetching = len(json) != 0
    logging.info("Retrieved %s agencies from dataXchange API", len(all_agencies))
    create_child_agencies(
        all_agencies=all_agencies, dry_run=dry_run, project_id=project_id
    )


def create_child_agencies(all_agencies: set, dry_run: bool, project_id: str) -> None:
    """
    Given a set of child agencies, creates child agencies of New Mexico Administrative Office of the Courts.
    """
    state_code = "US_NM"
    system = schema.System.COURTS_AND_PRETRIAL
    if project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION:
        super_agency_id = 534
    else:
        super_agency_id = 207
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
        ) as session:
            for agency in all_agencies:
                if dry_run is True:
                    logging.info("Would create %s", agency)
                    continue
                try:
                    AgencyInterface.create_agency(
                        session=session,
                        name=agency,
                        systems=[system],
                        state_code=state_code,
                        super_agency_id=super_agency_id,
                        fips_county_code=None,
                    )
                    logging.info("%s created", agency)
                except Exception as e:
                    logging.info(e)
            logging.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        get_child_agencies(dry_run=args.dry_run, project_id=args.project_id)
