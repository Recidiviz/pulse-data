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
""" Entrypoints for the ingest DAG Lock Management """
import argparse

from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.state_update_lock_manager import StateUpdateLockManager


# TODO(#27378): Delete this entrypoint once the legacy ingest DAG has been deleted
class IngestAcquireLockEntrypoint(EntrypointInterface):
    """Entrypoint for acquiring the ingest lock"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the ingest lock acquisition."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--state_code",
            help="The state code for which the lock needs to be acquired",
            type=StateCode,
            choices=list(StateCode),
            required=True,
        )

        parser.add_argument(
            "--ingest_instance",
            help="The ingest instance for which the lock needs to be acquired",
            type=DirectIngestInstance,
            choices=list(DirectIngestInstance),
            required=True,
        )

        parser.add_argument(
            "--lock_id",
            help="The lock id to acquire",
            type=str,
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        state_update_lock_manager = StateUpdateLockManager(
            state_code_filter=args.state_code, ingest_instance=args.ingest_instance
        )
        state_update_lock_manager.acquire_lock(lock_id=args.lock_id)


# TODO(#27378): Delete this entrypoint once the legacy ingest DAG has been deleted
class IngestReleaseLockEntrypoint(EntrypointInterface):
    """Entrypoint for releasing the ingest lock"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the ingest lock release."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--state_code",
            help="The state code for which the lock needs to be released",
            type=StateCode,
            choices=list(StateCode),
            required=True,
        )

        parser.add_argument(
            "--ingest_instance",
            help="The ingest instance for which the lock needs to be released",
            type=DirectIngestInstance,
            choices=list(DirectIngestInstance),
            required=True,
        )

        parser.add_argument(
            "--lock_id",
            help="The lock id to release",
            type=str,
            required=True,
        )

        return parser

    # TODO(#29058): add "fake" raw resource lock acquisition here behind gating and log
    # if we are starved
    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        state_update_lock_manager = StateUpdateLockManager(
            state_code_filter=args.state_code, ingest_instance=args.ingest_instance
        )
        state_update_lock_manager.release_lock(lock_id=args.lock_id)
