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
Cloud Run Job that runs the following scripts:
    - Justice Counts Data Pull
    - Agency Dashboard Data Pull
    - Superagency Data Pull
"""
import argparse
import logging
import time

import sentry_sdk
from oauth2client.client import GoogleCredentials

from recidiviz.justice_counts.jobs.csg_data_pull import generate_agency_summary_csv
from recidiviz.justice_counts.jobs.pull_agencies_with_published_data import (
    pull_agencies_with_published_capacity_and_cost_data,
)
from recidiviz.justice_counts.jobs.user_permissions_check import check_user_permissions
from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.params import str_to_bool

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
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


if __name__ == "__main__":
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )

    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    environment_str = (
        "PRODUCTION"
        if args.project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
        else "STAGING"
    )
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
    try:
        logger.info("Running CSG Data Pull")
        generate_agency_summary_csv(
            session=global_session,
            dry_run=args.dry_run,
            google_credentials=credentials,
            environment=environment_str,
        )
    except Exception as e:
        logger.exception("CSG Data Pull Script Failed: %s", str(e))
    try:
        time.sleep(60)
        logger.info("Running Dashboard Data Pull")
        pull_agencies_with_published_capacity_and_cost_data(
            session=global_session,
            environment=environment_str,
            google_credentials=credentials,
        )
    except Exception as e:
        logger.exception("Agency Dashboard Script Failed %s", str(e))
    try:
        time.sleep(60)
        logger.info("Closing DB Connection Before Running User Permissions Check")
        logger.info("Running User Permissions Checks")
        check_user_permissions(
            google_credentials=credentials,
            project_id=args.project_id,
            session=global_session,
        )
    except Exception as e:
        logger.exception("User Permissions Check: %s", str(e))
