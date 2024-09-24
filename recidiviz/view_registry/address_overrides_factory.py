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
import logging
from typing import Optional, Sequence

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import BigQueryAddress, BigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_datasets,
)


def address_overrides_for_view_builders(
    view_dataset_override_prefix: str,
    view_builders: Sequence[BigQueryViewBuilder],
    override_source_datasets: bool = False,
    dataflow_dataset_override: Optional[str] = None,
) -> BigQueryAddressOverrides:
    """Returns a class that provides a mapping of table/view addresses to the address
    they should be replaced with for all views that are regularly deployed by our
    standard deploy process. The mapping contains mappings for the view datasets and
    also the datasets of their corresponding materialized locations (if different).

    Overridden datasets all take the form of "<override prefix>_<original dataset_id>".

    If |override_source_datasets| is set, overrides of the same form will be added for
    all source datasets (e.g. `state`, `us_xx_raw_data`).

    If a |dataflow_dataset_override| is provided, will also override the address of all
    views in the DATAFLOW_METRICS_DATASET with that dataset value. If this is provided
    with |override_source_datasets|, the function will crash.
    """

    if override_source_datasets and dataflow_dataset_override:
        raise ValueError(
            "Cannot set both |override_source_datasets| and |dataflow_dataset_override|"
            " - creates conflicting information."
        )

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

    if override_source_datasets:
        for dataset in get_all_source_table_datasets():
            address_overrides_builder.register_sandbox_override_for_entire_dataset(
                dataset
            )

    if dataflow_dataset_override:
        logging.info(
            "Overriding [%s] dataset with [%s].",
            DATAFLOW_METRICS_DATASET,
            dataflow_dataset_override,
        )

        if (
            BigQueryAddressOverrides.format_sandbox_dataset(
                view_dataset_override_prefix, DATAFLOW_METRICS_DATASET
            )
            == dataflow_dataset_override
        ):
            address_overrides_builder.register_sandbox_override_for_entire_dataset(
                DATAFLOW_METRICS_DATASET
            )
        else:
            address_overrides_builder.register_custom_dataset_override(
                DATAFLOW_METRICS_DATASET, dataflow_dataset_override
            )
    return address_overrides_builder.build()
