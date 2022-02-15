# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
Important note: This script should be run on your local machine. It will not work
when run anywhere else.

This script runs all downgrade migrations for a given state database, so that when
new data is subsequently imported, it will not conflict due to duplicate definitions
of enums for instance.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.migrations.purge_state_db \
    --state-code US_MI \
    --ingest-instance SECONDARY \
    --project-id recidiviz-staging \
    [--purge-schema]
"""
import argparse
import logging

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.utils.script_helpers import (
    prompt_for_confirmation,
    run_command,
    run_command_streaming,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Purges all data from a remote PostgresQL database."
    )
    parser.add_argument(
        "--state-code",
        type=StateCode,
        choices=list(StateCode),
        help="Specifies the state where all downgrades will be run.",
        required=True,
    )
    parser.add_argument(
        "--ingest-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="Specifies the database version where all downgrades will be run.",
        required=True,
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--purge-schema",
        action="store_true",
        help="When set, runs the downgrade migrations.",
        default=False,
    )
    return parser


def get_hash_of_deployed_commit(project_id: str) -> str:
    """Returns the commit hash of the currently deployed version in the provided
    project.
    """

    # First make sure all tags are current locally
    run_command("git fetch --all --tags --prune --prune-tags", timeout_sec=30)

    get_tag_cmd = (
        f"gcloud app versions list --project={project_id} --hide-no-traffic "
        f"--service=default --format=yaml | yq .id | tr -d \\\" | tr '-' '.'"
        ' | sed "s/.alpha/-alpha/"'
    )

    get_commit_cmd = f"git rev-list -n 1 $({get_tag_cmd})"
    return run_command(get_commit_cmd, timeout_sec=30).strip()


def main(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    purge_schema: bool,
) -> None:
    """
    Invokes the main code path for running a downgrade.

    This checks for user validations that the database and branches are correct and then
    sshs into `prod-data-client` to run the downgrade migration.
    """
    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION
    if is_prod:
        logging.info("RUNNING AGAINST PRODUCTION\n")

    purge_str = (
        f"PURGE {state_code.value} DATABASE STATE IN "
        f"{'PROD' if is_prod else 'STAGING'} {ingest_instance.value}"
    )
    db_key = ingest_instance.database_key_for_state(state_code)

    prompt_for_confirmation(
        f"This script will PURGE all data for for [{state_code.value}] in DB [{db_key.db_name}].",
        purge_str,
    )
    if purge_schema:
        purge_schema_str = (
            f"RUN {state_code.value} DOWNGRADE MIGRATIONS IN "
            f"{'PROD' if is_prod else 'STAGING'} {ingest_instance.value}"
        )
        prompt_for_confirmation(
            f"This script will run all DOWNGRADE migrations for "
            f"[{state_code.value}] in DB [{db_key.db_name}].",
            purge_schema_str,
        )

    commit_hash = get_hash_of_deployed_commit(project_id=metadata.project_id())
    remote_commands = [
        "cd ~/pulse-data",
        # Git routes a lot to stderr that isn't really an error - redirect to stdout.
        "git fetch --all --tags --prune --prune-tags 2>&1",
        f"git checkout {commit_hash} 2>&1",
        (
            "pipenv run python -m recidiviz.tools.migrations.purge_state_db_remote_helper "
            f"--state-code {state_code.value} "
            f"--ingest-instance {ingest_instance.value} "
            f"--project-id {metadata.project_id()} "
            f"--commit-hash {commit_hash} "
            f"{'--purge-schema' if purge_schema else ''} "
        ),
    ]
    remote_command = " && ".join(remote_commands)
    local_command = (
        f"gcloud compute ssh prod-data-client --project recidiviz-123 --zone=us-east4-c "
        f'--command "{remote_command}"'
    )

    logging.info(
        "Running purge command on remote `prod-data-client`. "
        "You will be prompted for your `prod-data-client` password."
    )
    for stdout_line in run_command_streaming(local_command):
        print(stdout_line.rstrip())

    logging.info("Script complete.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(
            args.state_code,
            args.ingest_instance,
            args.purge_schema,
        )
