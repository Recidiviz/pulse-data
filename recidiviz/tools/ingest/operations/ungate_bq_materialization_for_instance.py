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
"""
Ungates BQ materialization in an ingest instance for a particular state.

python -m recidiviz.tools.ingest.operations.ungate_bq_materialization_for_instance \
    --project-id recidiviz-staging \
    --state-code US_PA \
    --ingest-instance SECONDARY

TODO(#11424): Delete this script once all states are migrated to BQ materialization.
"""

import argparse
import logging
import sys
from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.flash_database_tools import ungate_bq_materialization_for_instance
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        choices=GCP_PROJECTS,
        help="Used to select which GCP project against which to run this script.",
        required=True,
    )

    parser.add_argument(
        "--state-code",
        help="State to ungate BQ materialization for, in the form US_XX.",
        type=StateCode,
        choices=list(StateCode),
        required=True,
    )

    parser.add_argument(
        "--ingest-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="Instance to ungate BQ materialization for.",
        required=True,
    )

    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv[1:])
    with local_project_id_override(args.project_id):
        ungate_bq_materialization_for_instance(
            state_code=args.state_code, ingest_instance=args.ingest_instance
        )
