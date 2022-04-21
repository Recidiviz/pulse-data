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
"""Triggers the post-deploy tasks."""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.cloud_functions.cloudsql_to_bq_refresh_utils import (
    NO_HISTORICAL_DAG_FLAG,
    TRIGGER_HISTORICAL_DAG_FLAG,
)
from recidiviz.utils import pubsub_helper
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def main(trigger_historical_dag: int) -> None:
    """Sends a message to the PubSub topic to trigger the post-deploy CloudSQL to BQ
    refresh, with an arg indicating whether the refresh should trigger the
    historical DAG on completion."""
    if trigger_historical_dag:
        message = TRIGGER_HISTORICAL_DAG_FLAG
        logging.info("CloudSQL to BigQuery refresh will trigger historical pipelines.")
    else:
        message = NO_HISTORICAL_DAG_FLAG
        logging.info(
            "Historical pipelines will not be triggered - no relevant code changes."
        )

    pubsub_helper.publish_message_to_topic(
        topic="v1.trigger_post_deploy_cloudsql_to_bq_refresh_state",
        message=message,
    )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--trigger-historical-dag",
        type=int,
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        main(known_args.trigger_historical_dag)
