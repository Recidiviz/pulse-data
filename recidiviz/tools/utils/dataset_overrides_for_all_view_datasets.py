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
"""Returns a dictionary mapping dataset_ids to the dataset name they should be replaced with for all view datasets
in view_datasets. If a |dataflow_dataset_override| is provided, will override the DATAFLOW_METRICS_DATASET with
the provided value.
"""
from typing import Dict, Optional
import logging

from recidiviz.big_query.view_update_manager import VIEW_BUILDERS_BY_NAMESPACE
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    DATAFLOW_METRICS_DATASET,
)


def dataset_overrides_for_all_view_datasets(
    view_dataset_override_prefix: str, dataflow_dataset_override: Optional[str] = None
) -> Dict[str, str]:
    dataset_overrides = {}
    for view_builders in VIEW_BUILDERS_BY_NAMESPACE.values():
        for builder in view_builders:
            dataset_id = builder.dataset_id

            if (
                dataset_id == DATAFLOW_METRICS_MATERIALIZED_DATASET
                and dataflow_dataset_override is None
            ):
                # Only override the DATAFLOW_METRICS_MATERIALIZED_DATASET if the DATAFLOW_METRICS_DATASET will also
                # be overridden
                continue

            dataset_overrides[dataset_id] = (
                view_dataset_override_prefix + "_" + dataset_id
            )

    if dataflow_dataset_override:
        logging.info(
            "Overriding [%s] dataset with [%s].",
            DATAFLOW_METRICS_DATASET,
            dataflow_dataset_override,
        )

        dataset_overrides[DATAFLOW_METRICS_DATASET] = dataflow_dataset_override

    return dataset_overrides
