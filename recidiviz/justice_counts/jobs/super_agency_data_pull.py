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
"""Job that writes to the Justice Counts Superagencies google spreadsheet with data about
all superagencies and their child agencies.

pipenv run python -m recidiviz.justice_counts.jobs.super_agency_data_pull \
  --project-id=PROJECT_ID \
  --dry-run=true \
  --credentials-path=<path>
"""
import argparse
import logging
from collections import defaultdict
from typing import Any, Dict, List

import pandas as pd
import sentry_sdk
from google.oauth2.service_account import Credentials
from oauth2client.client import GoogleCredentials

from recidiviz.justice_counts.control_panel.utils import write_data_to_spreadsheet
from recidiviz.justice_counts.utils.constants import (
    AGENCIES_TO_EXCLUDE,
    JUSTICE_COUNTS_SENTRY_DSN,
)
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

# Spreadsheet Name: Justice Counts Superagencies
# https://docs.google.com/spreadsheets/d/1-w-ZY9uCrbEBX9R6MHRY0Ukv86KuP_aYZx6Bs9ZCqKw/edit#gid=0
SUPERAGENCY_SPREADSHEET_ID = "1-w-ZY9uCrbEBX9R6MHRY0Ukv86KuP_aYZx6Bs9ZCqKw"

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
        required=False,
    )
    parser.add_argument(
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


def get_superagency_data(
    session: Session,
) -> Dict[str, List[str]]:
    """Retrieve superagency and child agency data from our database"""
    original_agencies = session.execute(
        "select * from source where type = 'agency' and LOWER(name) not like '%test%'"
    ).all()

    agencies_to_exclude = AGENCIES_TO_EXCLUDE.keys()
    super_agencies = [
        a
        for a in original_agencies
        if a["id"] not in agencies_to_exclude and bool(a["is_superagency"])
    ]
    super_agency_name_to_child_agency_names = defaultdict(List)
    for super_agency in super_agencies:
        super_agency_id = super_agency["id"]
        query = f"select name from source where super_agency_id = '{super_agency_id}'"
        child_agency_tuples = session.execute(query).all()
        child_agency_names = [child_agency[0] for child_agency in child_agency_tuples]
        super_agency_name_to_child_agency_names[
            super_agency["name"]
        ] = child_agency_names

    return super_agency_name_to_child_agency_names


def generate_superagency_summary(
    session: Session,
    dry_run: bool,
    google_credentials: Any,
) -> None:
    super_agency_name_to_child_agency_names = get_superagency_data(session=session)

    for (
        super_agency_name,
        child_agencies,
    ) in super_agency_name_to_child_agency_names.items():
        df = pd.DataFrame(child_agencies, columns=["child_agencies"]).sort_values(
            by="child_agencies"
        )
        if dry_run is False:
            write_data_to_spreadsheet(
                spreadsheet_id=SUPERAGENCY_SPREADSHEET_ID,
                google_credentials=google_credentials,
                df=df,
                columns=["child_agencies"],
                new_sheet_title=super_agency_name,
                logger=logger,
                overwrite_sheets=True,
                index=1,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )
    args = create_parser().parse_args()
    if args.project_id is not None:
        # When running locally, point to JSON file with service account credentials.
        # The service account has access to the spreadsheet with editor permissions.
        credentials = Credentials.from_service_account_file(args.credentials_path)
        with local_project_id_override(args.project_id):
            schema_type = SchemaType.JUSTICE_COUNTS
            database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)

            with cloudsql_proxy_control.connection(
                schema_type=schema_type,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            ):
                with SessionFactory.for_proxy(
                    database_key=database_key,
                    secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                    autocommit=False,
                ) as global_session:
                    generate_superagency_summary(
                        session=global_session,
                        dry_run=args.dry_run,
                        google_credentials=credentials,
                    )
    else:
        # When running via Cloud Run Job, GoogleCredentials.get_application_default()
        # will use the service account assigned to the Cloud Run Job.
        # The service account has access to the spreadsheet with editor permissions.
        credentials = GoogleCredentials.get_application_default()
        database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.JUSTICE_COUNTS,
        )
        justice_counts_engine = SQLAlchemyEngineManager.init_engine(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        )
        global_session = Session(bind=justice_counts_engine)
        generate_superagency_summary(
            session=global_session, dry_run=args.dry_run, google_credentials=credentials
        )
