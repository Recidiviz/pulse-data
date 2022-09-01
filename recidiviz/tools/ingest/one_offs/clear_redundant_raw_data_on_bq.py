#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""
Local script for clearing out redundant raw data on BQ for states with frequent historical uploads (and updates Postgres
metadata accordingly).

Example Usage:
    python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run True --project-id=recidiviz-staging
"""
import argparse
import logging
import sys

from recidiviz.common.constants.states import StateCode
from recidiviz.tools.utils.script_helpers import (
    prompt_for_confirmation,
    run_command_streaming,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


# TODO(#14127): delete this script once raw data pruning is live.
def main(dry_run: bool, state_code: StateCode, project_id: str) -> None:
    """Main flow to clear redundant data on bq and update associated postgres metadata."""
    if not dry_run:
        prompt_for_confirmation(
            f"Have you confirmed that there are NO tasks running for this state in {project_id}: "
            f" https://console.cloud.google.com/cloudtasks?referrer=search&project={project_id}?"
        )
        prompt_for_confirmation(
            "Pause queues: "
            f"https://{project_id}.ue.r.appspot.com/admin/ingest_operations/key_actions?stateCode={state_code.value}?"
        )
        prompt_for_confirmation(
            f"Are you sure this state receives frequent historical uploads {state_code.value}?"
        )

    remote_commands = [
        "cd ~/pulse-data",
        "git checkout main",
        "git pull",
        (
            "pipenv run python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq_remote_helper "
            f"--dry-run {dry_run} "
            f"--state-code {state_code.value} "
            f"--project-id {project_id} "
        ),
    ]
    remote_command = " && ".join(remote_commands)
    local_command = (
        f"gcloud compute ssh prod-data-client --project recidiviz-123 --zone=us-east4-c "
        f'--command "{remote_command}"'
    )

    for stdout_line in run_command_streaming(local_command):
        print(stdout_line.rstrip())

    if not dry_run:
        prompt_for_confirmation(
            f"Unpause queues: "
            f"https://{project_id}.ue.r.appspot.com/admin/ingest_operations/key_actions?stateCode={state_code.value}?"
        )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    parser.add_argument("--state-code", type=StateCode, required=True)

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    return parser


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(args.dry_run, args.state_code, args.project_id)
