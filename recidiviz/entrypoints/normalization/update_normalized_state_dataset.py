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

from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.calculation_data_storage_manager import (
    execute_update_normalized_state_dataset,
)


class UpdateNormalizedStateEntrypoint(EntrypointInterface):
    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the Cloud SQL to BQ refresh process."""
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--state_code_filter",
            help="the state to update",
            type=StateCode,
            choices=list(StateCode),
        )
        parser.add_argument(
            "--ingest_instance",
            help="The ingest instance data is from",
            type=DirectIngestInstance,
            choices=list(DirectIngestInstance),
            required=True,
        )
        parser.add_argument(
            "--sandbox_prefix",
            help="The sandbox prefix for which the refresh needs to write to",
            type=str,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        execute_update_normalized_state_dataset(
            ingest_instance=args.ingest_instance,
            state_codes_filter=[args.state_code_filter]
            if args.state_code_filter
            else None,
            sandbox_dataset_prefix=args.sandbox_prefix,
        )
