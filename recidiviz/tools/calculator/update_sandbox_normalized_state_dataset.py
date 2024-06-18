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
"""Script for updating a sandbox version of the `normalized_state` dataset.

For normalized entities this will copy the table from the sandbox version of the state
specific normalized dataset for the specified state
(`{sandbox}_{state_code}_normalized_state`, e.g. `test_us_mo_normalized_state`). For
not normalized entities, the sandbox version of the `state` dataset will be copied. All
of the data for those tables will be included, even if they include data for states
other than the specified state.

Typically, this is used as part of the secondary rerun validation flow, after:

1. `load_postgres_to_sandbox` has been used to load secondary data into the sandbox
   state dataset.
2. The normalization pipeline has been run and output data into a sandbox state specific
   normalized state dataset.

Then this script can be used to create a unified sandbox version of the `normalized_state`
dataset, pulling data from the unnormalized sandbox state dataset for entities that are
not normalized and from the sandbox normalized state datasets for entities that are.

After this script has been run, you can run metric pipelines that depend on normalized
state, providing the sandbox normalized state dataset that this script produced.

Ex.
python -m recidiviz.tools.calculator.update_sandbox_normalized_state_dataset \
    --project_id recidiviz-staging \
    --state_code US_XX \
    --input_state_dataset us_xx_state \
    --input_normalized_state_dataset my_prefix_us_xx_normalized_state \
    --output_sandbox_prefix my_prefix
"""

import argparse
import logging

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.calculation_data_storage_manager import (
    update_normalized_state_dataset,
)
from recidiviz.pipelines.normalization.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Create an argument parser for this script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--state_code",
        dest="state_code",
        type=StateCode,
        choices=list(StateCode),
        help="State code to load into the sandbox normalized_state dataset",
    )

    parser.add_argument(
        "--input_state_dataset",
        dest="input_state_dataset",
        help=(
            "The non-normalized state dataset that this script should read from "
            "(for entities that are not produced by the normalization pipelines)."
        ),
        type=str,
        required=True,
    )

    parser.add_argument(
        "--input_normalized_state_dataset",
        dest="input_normalized_state_dataset",
        help="The normalized state dataset that this script should read from.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--output_sandbox_prefix",
        dest="output_sandbox_prefix",
        help="The sandbox prefix for the output of this script.",
        type=str,
        required=True,
    )

    return parser


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        state_code = args.state_code
        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix=args.output_sandbox_prefix,
        )
        builder.register_custom_dataset_override(
            normalized_state_dataset_for_state_code(state_code),
            args.input_normalized_state_dataset,
            force_allow_custom=True,
        )
        builder.register_custom_dataset_override(
            STATE_BASE_DATASET, args.input_state_dataset, force_allow_custom=True
        )
        builder.register_sandbox_override_for_entire_dataset(NORMALIZED_STATE_DATASET)
        address_overrides = builder.build()

        update_normalized_state_dataset(
            state_codes_filter=frozenset({state_code}),
            address_overrides=address_overrides,
        )
