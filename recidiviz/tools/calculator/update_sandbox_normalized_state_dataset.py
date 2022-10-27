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
    --state_code US_MO \
    --sandbox_dataset_prefix colin_foo
"""

import argparse
import logging
from typing import FrozenSet

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.calculation_data_storage_manager import (
    update_normalized_state_dataset,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.common.constants import states
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def build_address_overrides_for_update(
    dataset_override_prefix: str, states_to_override: FrozenSet[states.StateCode]
) -> BigQueryAddressOverrides:

    overrides_builder = BigQueryAddressOverrides.Builder(
        sandbox_prefix=dataset_override_prefix
    )
    overrides_builder.register_sandbox_override_for_entire_dataset(
        dataset_config.STATE_BASE_DATASET
    )
    for state_code in states_to_override:
        overrides_builder.register_sandbox_override_for_entire_dataset(
            normalized_state_dataset_for_state_code(state_code)
        )
    overrides_builder.register_sandbox_override_for_entire_dataset(
        dataset_config.NORMALIZED_STATE_DATASET
    )

    return overrides_builder.build()


def create_parser() -> argparse.ArgumentParser:
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
        choices=[state.value for state in states.StateCode],
        help="State code to use when filtering dataset to create metrics export",
        type=str,
        required=False,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="A prefix to append to all names of the datasets where these views will be loaded.",
        type=str,
        required=True,
    )

    return parser


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        state_codes = frozenset({states.StateCode(args.state_code)})
        update_normalized_state_dataset(
            state_codes_filter=state_codes,
            address_overrides=build_address_overrides_for_update(
                dataset_override_prefix=args.sandbox_dataset_prefix,
                states_to_override=state_codes,
            ),
        )
