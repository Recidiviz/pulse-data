# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
A script to run the ingest DAG for a specific state and instance.
Run:
    python -m recidiviz.tools.airflow.trigger_state_specific_ingest_dag \
       --state-code [state_code] \
       --ingest-instance [ingest_instance]
"""
import argparse
import logging

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.trigger_dag_helpers import trigger_ingest_dag_pubsub


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(description="Run an ingest DAG for a single state")

    parser.add_argument(
        "--state-code",
        required=True,
        type=StateCode,
        choices=list(StateCode),
        help="Ingest DAG will run for this region",
    )

    parser.add_argument(
        "--ingest-instance",
        required=True,
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="The ingest instance to run the DAG with.",
    )

    return parser


def trigger_state_specific_ingest_dag(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
) -> None:
    trigger_ingest_dag_pubsub(ingest_instance, state_code)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        trigger_state_specific_ingest_dag(args.state_code, args.ingest_instance)
