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
"""Entrypoint for updating the `state` dataset in BigQuery with outputs from
state-specific ingest pipelines.
"""

import argparse
import datetime
import uuid
from typing import Optional

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.success_persister import RefreshBQDatasetSuccessPersister
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.union_dataflow_ingest import (
    combine_ingest_sources_into_single_state_dataset,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema
from recidiviz.pipelines.state_update_lock_manager import StateUpdateLockManager
from recidiviz.utils.environment import gcp_only

LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT = 60 * 60 * 4  # 4 hours


@gcp_only
def execute_state_dataset_refresh(
    ingest_instance: DirectIngestInstance,
    sandbox_prefix: Optional[str],
) -> None:
    """Unions all the `us_xx_state_primary` or `us_xx_state_secondary` datasets into
    a single output `state` dataset. Results are written to a sandbox version of `state`
    if a |sandbox_prefix| is provided.

    If the instance is SECONDARY a sandbox_prefix must be provided.
    """
    if ingest_instance == DirectIngestInstance.SECONDARY and not sandbox_prefix:
        raise ValueError(
            "Refresh can only proceed for secondary databases into a sandbox."
        )

    state_update_lock_manager = StateUpdateLockManager(ingest_instance)
    state_update_lock_manager_lock_id = str(uuid.uuid4())

    try:
        state_update_lock_manager.acquire_lock(
            lock_id=state_update_lock_manager_lock_id,
            lock_wait_timeout=LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT,
        )
        start = datetime.datetime.now()

        combine_ingest_sources_into_single_state_dataset(
            ingest_instance=ingest_instance,
            tables=list(get_all_table_classes_in_schema(SchemaType.STATE)),
            output_sandbox_prefix=sandbox_prefix,
        )

        end = datetime.datetime.now()
        runtime_sec = int((end - start).total_seconds())
        success_persister = RefreshBQDatasetSuccessPersister(
            bq_client=BigQueryClientImpl()
        )
        success_persister.record_success_in_bq(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=ingest_instance,
            dataset_override_prefix=sandbox_prefix,
            runtime_sec=runtime_sec,
        )
    finally:
        state_update_lock_manager.release_lock(state_update_lock_manager_lock_id)


class UpdateStateEntrypoint(EntrypointInterface):
    """Entrypoint for updating the `state` dataset in BigQuery with outputs from state-specific ingest pipelines."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the state dataset update process."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--ingest_instance",
            help=(
                "The ingest instance for the specified state dataset update. Determines whether we read from "
                "us_xx_primary or us_xx_secondary datasets."
            ),
            type=DirectIngestInstance,
            choices=list(DirectIngestInstance),
            required=True,
        )
        parser.add_argument(
            "--sandbox_prefix",
            help="The sandbox prefix for the update output dataset.",
            type=str,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        execute_state_dataset_refresh(
            ingest_instance=args.ingest_instance,
            sandbox_prefix=args.sandbox_prefix,
        )
