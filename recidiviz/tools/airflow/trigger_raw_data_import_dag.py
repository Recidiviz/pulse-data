# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
A script to run the raw data import DAG for a specific state and instance.
Run:
    python -m recidiviz.tools.airflow.trigger_raw_data_import_dag \
       --project-id [project_id] \
       --raw-data-instance [raw_data_instance] \
       [--state-code [state_code]]
"""
import argparse
import logging

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.trigger_dag_helpers import trigger_raw_data_import_dag_pubsub


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Run a raw data import DAG for a single state"
    )

    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Which GCP project to run the raw data import DAG in.",
        required=True,
    )
    parser.add_argument(
        "--state-code-filter",
        required=False,
        type=StateCode,
        choices=list(StateCode),
        help="The raw data import DAG will only run for this region",
    )

    parser.add_argument(
        "--raw-data-instance",
        required=True,
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="The raw data instance to run the raw data import DAG with.",
    )

    return parser


def trigger_state_specific_raw_data_import_dag(
    project_id: str,
    state_code_filter: StateCode | None,
    raw_data_instance: DirectIngestInstance,
) -> None:
    prompt_for_confirmation(
        f"Trigger dag in [{project_id}] for [{raw_data_instance.value}] for [{state_code_filter.value if state_code_filter else 'all states'}]"
    )
    trigger_raw_data_import_dag_pubsub(
        raw_data_instance=raw_data_instance, state_code_filter=state_code_filter
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        trigger_state_specific_raw_data_import_dag(
            args.project_id, args.state_code_filter, args.raw_data_instance
        )
