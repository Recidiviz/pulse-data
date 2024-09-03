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
""" Entrypoints for the Check Raw Data Flashing Management """
import argparse

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


# TODO(#29058): add gated check w/ new DirectIngestRawDataFlashStatusManager, maybe move
# this into a sql query generator??
def _verify_raw_data_flashing_not_in_progress(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> None:
    """
    Checks the raw data processing status for the given state code and ingest instance.
     Raises an exception if the raw data flashing is in progress.
    """
    status_manager = DirectIngestInstanceStatusManager(
        region_code=state_code.value,
        ingest_instance=ingest_instance,
    )

    if status_manager.get_current_status() == DirectIngestStatus.FLASH_IN_PROGRESS:
        raise ValueError(
            f"Raw data flashing is in progress for {state_code.value} {ingest_instance.value}."
        )


class IngestCheckRawDataFlashingEntrypoint(EntrypointInterface):
    """Entrypoint for checking the raw data flashing"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the raw data flashing check."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--state_code",
            help="The state code for which the raw data flashing status needs to be checked",
            type=StateCode,
            choices=list(StateCode),
            required=True,
        )

        parser.add_argument(
            "--ingest_instance",
            help="The ingest instance for which the raw data flashing status needs to be checked",
            type=DirectIngestInstance,
            choices=list(DirectIngestInstance),
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        """Runs the raw data flashing check."""
        state_code = args.state_code
        ingest_instance = args.ingest_instance

        _verify_raw_data_flashing_not_in_progress(state_code, ingest_instance)
