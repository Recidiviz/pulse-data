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
"""Script for a Validation to occur for a given state to be called only
within the Airflow DAG's KubernetesPodOperator."""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.validation_manager import execute_validation_request


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parse arguments for the validation script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        help="Project ID to validate",
        type=str,
        choices=GCP_PROJECTS,
        required=True,
    )
    parser.add_argument(
        "--state_code",
        help="State code to validate",
        type=StateCode,
        choices=list(StateCode),
        required=True,
    )
    parser.add_argument(
        "--ingest_instance",
        help="Ingest instance to validate",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        required=True,
    )
    parser.add_argument(
        "--sandbox_prefix",
        help="Sandbox prefix to validate",
        type=str,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args, unknown_args = parse_arguments(sys.argv[1:])
    with local_project_id_override(args.project_id):
        execute_validation_request(
            state_code=args.state_code,
            ingest_instance=args.ingest_instance,
            sandbox_prefix=args.sandbox_prefix,
        )
