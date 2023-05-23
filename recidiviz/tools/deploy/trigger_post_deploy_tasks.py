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
import json
import logging
import sys
from typing import List, Optional, Tuple

from recidiviz.common.constants.states import StateCode
from recidiviz.utils import pubsub_helper
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def main(
    state_code_filter: Optional[StateCode], sandbox_dataset_prefix: Optional[str]
) -> None:
    """Sends a message to the PubSub topic to trigger the post-deploy CloudSQL to BQ
    refresh, which then will trigger the historical DAG on completion."""

    logging.info("Triggering the historical DAG.")
    pubsub_helper.publish_message_to_topic(
        topic="v1.calculator.trigger_calculation_pipelines",
        message=json.dumps(
            {
                "state_code_filter": state_code_filter,
                "sandbox_dataset_prefix": sandbox_dataset_prefix,
            }
        ),
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
        "--state_code_filter",
        type=StateCode,
        help="The state code of the state to run the historical DAG for.",
        required=False,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="The prefix of the sandbox dataflow_metrics dataset.",
        type=str,
        required=False,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        main(known_args.state_code_filter, known_args.sandbox_dataset_prefix)
