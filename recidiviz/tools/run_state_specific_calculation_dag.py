# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
A script to run the calculation DAG in a secondary environment for a specific state. Once the script is run,
open Airflow in staging (go/airflow-staging) and you can find the sandbox prefix the dag created
within the initialize_dag task group if you did not provide one as an argument.

Run:
    python -m recidiviz.tools.run_state_specific_calculation_dag \
       --state-code [state_code] \
       --ingest-instance [ingest_instance] \
       --sandbox-prefix [SANDBOX_PREFIX] (Optional)
"""
import argparse
import logging
from datetime import datetime
from typing import Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.deploy.trigger_post_deploy_tasks import (
    trigger_calculation_dag_pubsub,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Run the calculation DAG in a secondary environment."
    )

    parser.add_argument(
        "--state-code",
        required=True,
        type=StateCode,
        choices=list(StateCode),
        help="Validations will run for this region.",
    )

    parser.add_argument(
        "--ingest-instance",
        required=True,
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="The ingest instance to run the DAG with.",
    )

    parser.add_argument(
        "--sandbox-prefix",
        required=False,
        type=str,
        help="The prefix for the sandbox datasets. One will be generated automatically for SECONDARY runs if one is not already provided.",
    )

    return parser


def main(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    sandbox_prefix: Optional[str] = None,
) -> None:
    if ingest_instance == DirectIngestInstance.SECONDARY and not sandbox_prefix:
        sandbox_prefix = f"{state_code.value.lower()}_{ingest_instance.value.lower()}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        logging.info("Setting sandbox prefix to: %s", sandbox_prefix)

    trigger_calculation_dag_pubsub(ingest_instance, state_code, sandbox_prefix)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        main(args.state_code, args.ingest_instance, args.sandbox_prefix)
