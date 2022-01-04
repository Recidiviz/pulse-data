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
"""Utils for deriving a dictionary mapping dataset_ids to the dataset name they should
be replaced with.
"""
import logging
from typing import Dict, Optional, Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders


def dataset_overrides_for_deployed_view_datasets(
    project_id: str,
    view_dataset_override_prefix: str,
    dataflow_dataset_override: Optional[str] = None,
) -> Dict[str, str]:
    """Returns a dictionary mapping dataset_ids to the dataset name they should be
    replaced with for all views that are regularly deployed by our standard deploy
    process. The map contains mappings for the view datasets and also the datasets of
    their corresponding materialized locations (if different).

    Overridden datasets all take the form of "<override prefix>_<original dataset_id>".

    If a |dataflow_dataset_override| is provided, will also override the
    DATAFLOW_METRICS_DATASET with the provided value.
    """

    all_view_builders = []
    for builder in deployed_view_builders(project_id):
        if (
            builder.dataset_id == DATAFLOW_METRICS_MATERIALIZED_DATASET
            and dataflow_dataset_override is None
        ):
            # The DATAFLOW_METRICS_MATERIALIZED_DATASET dataset is a super expensive
            # dataset to reproduce and materialize, since it queries from all of the
            # DATAFLOW_METRICS_DATASET tables. We have this special case to avoid
            # making a sandbox version of this dataset unless the user is explicitly
            # testing a change where the views should read from a dataflow metrics
            # dataset that's different than usual (aka if
            # `dataflow_dataset_override` is not None).
            continue

        all_view_builders.append(builder)

    dataset_overrides = dataset_overrides_for_view_builders(
        view_dataset_override_prefix,
        all_view_builders,
        dataflow_dataset_override=dataflow_dataset_override,
    )

    return dataset_overrides


def dataset_overrides_for_view_builders(
    view_dataset_override_prefix: str,
    view_builders: Sequence[BigQueryViewBuilder],
    dataflow_dataset_override: Optional[str] = None,
) -> Dict[str, str]:
    """Returns a dictionary mapping dataset_ids to the dataset name they should be
    replaced with for the given list of view_builders. The map contains mappings for the
    view datasets and also the datasets of their corresponding materialized locations
    (if different).

    Overridden datasets all take the form of "<override prefix>_<original dataset_id>".

    If a |dataflow_dataset_override| is provided, will also override the
    DATAFLOW_METRICS_DATASET with the provided value.
    """
    dataset_overrides: Dict[str, str] = {}
    for builder in view_builders:
        _add_override(
            dataset_overrides, view_dataset_override_prefix, builder.dataset_id
        )
        if builder.materialized_address_override:
            materialized_dataset_id = builder.materialized_address_override.dataset_id
            _add_override(
                dataset_overrides, view_dataset_override_prefix, materialized_dataset_id
            )

    if dataflow_dataset_override:
        logging.info(
            "Overriding [%s] dataset with [%s].",
            DATAFLOW_METRICS_DATASET,
            dataflow_dataset_override,
        )

        dataset_overrides[DATAFLOW_METRICS_DATASET] = dataflow_dataset_override

    return dataset_overrides


def _add_override(overrides: Dict[str, str], prefix: str, dataset_id: str) -> None:
    overrides[dataset_id] = prefix + "_" + dataset_id
