# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
This job identifies duplicate datapoints in the Justice Counts database. 
If there are duplicate datapoints, it will message #jc-eng-only and log 
the duplicate datapoints in the job log. 

Usage:
python -m recidiviz.justice_counts.jobs.duplicate_datapoints_job \
  --project-id=justice-counts-staging
"""

import argparse
import logging

import requests
import sentry_sdk
from sqlalchemy.orm import Session

from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.justice_counts.delete_duplicate_datapoints import (
    get_duplicate_datapoints,
)
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
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
    parser.add_argument(
        "--dry-run",
        type=str_to_bool,
        default=False,
        help="Run the script without making changes",
    )
    return parser


def surface_duplicate_datapoints(
    session: Session, project_id: str, dry_run: bool
) -> None:
    duplicate_datapoints = get_duplicate_datapoints(session=session, dry_run=dry_run)
    if len(duplicate_datapoints) == 0:
        return

    slack_channel_id = "C05C9CND8N7"  # slack id for #jc-eng-only
    slack_token = get_secret("deploy_slack_bot_authorization_token")
    if not slack_token:
        logging.error("Couldn't find `deploy_slack_bot_authorization_token`")
        return

    text = f":rotating_light: *Duplicate Report Datapoints Found* :rotating_light: \nDuplicate report datapoints have been found in the Datapoint table in {project_id}.\nPlease check the detect-duplicate-datapoints logs for details."

    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            json={"text": text, "channel": slack_channel_id},
            headers={"Authorization": "Bearer " + slack_token},
            timeout=60,
        )
        logging.info("Response from Slack API call: %s", response)
    except Exception as e:
        logging.exception("Error when calling Slack API: %s", str(e))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )
    args = create_parser().parse_args()

    database_key = SQLAlchemyDatabaseKey.for_schema(
        SchemaType.JUSTICE_COUNTS,
    )
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(
        database_key=database_key,
        secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
    )
    job_session = Session(bind=justice_counts_engine)
    surface_duplicate_datapoints(
        session=job_session, project_id=args.project_id, dry_run=args.dry_run
    )
