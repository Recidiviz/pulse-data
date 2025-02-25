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
"""Utils for deriving a mapping of BigQueryAddress to to the sandbox BigQueryAddress
they should be replaced with.
"""
from typing import Sequence

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import BigQueryAddress, BigQueryViewBuilder
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.utils import metadata


def address_overrides_for_view_builders(
    view_dataset_override_prefix: str,
    view_builders: Sequence[BigQueryViewBuilder],
) -> BigQueryAddressOverrides:
    """
    Returns a class that specifies that, for each view in |view_builders|, when that
    view's address OR materialized_address is encountered (e.g. as a parent view in a
    query), the address should be replaced with a sandbox address that uses the given
    view_dataset_override_prefix.
    """
    address_overrides_builder = BigQueryAddressOverrides.Builder(
        sandbox_prefix=view_dataset_override_prefix
    )
    for builder in view_builders:
        address_overrides_builder.register_sandbox_override_for_address(
            BigQueryAddress(dataset_id=builder.dataset_id, table_id=builder.view_id)
        )
        if builder.materialized_address:
            address_overrides_builder.register_sandbox_override_for_address(
                builder.materialized_address
            )

    return address_overrides_builder.build()


def address_overrides_for_input_source_tables(
    input_source_table_dataset_overrides: dict[str, str]
) -> "BigQueryAddressOverrides":
    """
    Given a mapping of dataset_id -> override dataset_id for a set of source tables,
    returns a BigQueryAddressOverrides object that encodes those overrides.

    Throws if the source table datasets to override are not valid source table datasets.
    """

    valid_source_table_datasets = get_source_table_datasets(metadata.project_id())
    builder = BigQueryAddressOverrides.Builder(sandbox_prefix=None)
    for (
        original_dataset,
        override_dataset,
    ) in input_source_table_dataset_overrides.items():
        if original_dataset not in valid_source_table_datasets:
            raise ValueError(
                f"Dataset [{original_dataset}] is not a valid source table dataset - "
                f"cannot override."
            )

        if original_dataset == override_dataset:
            raise ValueError(
                f"Input dataset override for [{original_dataset}] must be "
                f"different than the original dataset."
            )
        builder.register_custom_dataset_override(
            original_dataset, override_dataset, force_allow_custom=True
        )
    return builder.build()
