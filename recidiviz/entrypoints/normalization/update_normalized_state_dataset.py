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
from recidiviz.persistence.database.bq_refresh.update_normalized_state_dataset import (
    combine_sources_into_single_normalized_state_dataset,
)
from recidiviz.utils.environment import gcp_only


# TODO(#29519): Delete this endpoint when we move the `normalized_state` dataset refresh
#  into the view graph.
@gcp_only
def execute_update_normalized_state_dataset(
    output_sandbox_prefix: str | None,
    state_codes_filter: list[StateCode] | None,
) -> None:
    combine_sources_into_single_normalized_state_dataset(
        output_sandbox_prefix=output_sandbox_prefix,
        # TODO(#26138): We will need to set something here in order to do
        #  end-to-end sandbox runs.
        input_dataset_overrides=None,
        state_codes_filter=state_codes_filter,
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
            "--sandbox_prefix",
            help="The sandbox prefix for which the refresh needs to write to",
            type=str,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        execute_update_normalized_state_dataset(
            state_codes_filter=[args.state_code_filter]
            if args.state_code_filter
            else None,
            output_sandbox_prefix=args.sandbox_prefix,
        )
