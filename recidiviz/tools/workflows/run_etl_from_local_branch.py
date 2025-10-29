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
"""A script for running the Workflows ETL for a given file and state

To run the ETL for a specific file and state, run:
    python -m recidiviz.tools.workflows.run_etl_from_local_branch \
       --filename [file name]
       --state_code [state_code]

"""
import argparse
import asyncio
import logging

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.routes import get_workflows_delegates


async def main(
    filename: str,
    state_code: str,
) -> None:
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        for delegate in get_workflows_delegates(StateCode(state_code)):
            try:
                if delegate.supports_file(filename):
                    await delegate.run_etl(filename)
            except ValueError:
                logging.info(
                    "Error running Firestore ETL for file %s for state_code %s",
                    filename,
                    state_code,
                )


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--filename",
        dest="filename",
        help="file name for the collection you want to update",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--state_code",
        dest="state_code",
        help="State code of the collection you want to update",
        type=str,
        required=True,
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(
        main(
            filename=args.filename,
            state_code=args.state_code,
        )
    )
