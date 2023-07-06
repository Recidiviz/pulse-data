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
"""Script for update_normalized_state_dataset
to be called only within the Airflow DAG's KubernetesPodOperator."""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.calculation_data_storage_manager import (
    execute_update_normalized_state_dataset,
)
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses arguments for the Cloud SQL to BQ refresh process."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        help="The project that the refresh should occur in",
        choices=GCP_PROJECTS,
        required=True,
    )
    parser.add_argument(
        "--state_code_filter",
        help="the state to update",
        type=StateCode,
        choices=list(StateCode),
    )
    parser.add_argument(
        "--sandbox_prefix",
        help="The sandbox prefix for which the refresh needs to write to",
        type=str,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(project_id_override=known_args.project_id):
        execute_update_normalized_state_dataset(
            [known_args.state_code_filter] if known_args.state_code_filter else None,
            known_args.sandbox_prefix,
        )
