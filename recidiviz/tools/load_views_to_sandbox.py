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
"""A script for writing all regularly updated views in BigQuery to a set of temporary datasets. Used during development
to test updates to views.

This can be run on-demand whenever locally with the following command:
    python -m recidiviz.tools.load_views_to_sandbox \
        --project_id [PROJECT_ID] \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
        --dataflow_dataset_override [DATAFLOW_DATASET_OVERRIDE] \
        --refresh_materialized_tables_only [True,False]
"""
import argparse
import logging
import sys
from typing import List, Tuple, Optional

from recidiviz.big_query.view_update_manager import (
    create_dataset_and_deploy_views_for_view_builders,
    rematerialize_views_for_namespace,
)
from recidiviz.view_registry.deployed_views import DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.tools.utils.dataset_overrides_for_all_view_datasets import (
    dataset_overrides_for_all_view_datasets,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


def load_views_to_sandbox(
    sandbox_dataset_prefix: str,
    dataflow_dataset_override: Optional[str] = None,
    refresh_materialized_tables_only: bool = False,
) -> None:
    """Loads all views into sandbox datasets prefixed with the sandbox_dataset_prefix."""
    sandbox_dataset_overrides = dataset_overrides_for_all_view_datasets(
        view_dataset_override_prefix=sandbox_dataset_prefix,
        dataflow_dataset_override=dataflow_dataset_override,
    )

    for namespace, builders in DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE.items():
        builders_to_update = [
            builder
            for builder in builders
            if dataflow_dataset_override is not None
            # Only update views in the DATAFLOW_METRICS_MATERIALIZED_DATASET if the dataflow_dataset_override is set
            or builder.dataset_id != DATAFLOW_METRICS_MATERIALIZED_DATASET
        ]

        if refresh_materialized_tables_only:
            rematerialize_views_for_namespace(
                bq_view_namespace=namespace,
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                candidate_view_builders=builders_to_update,
                dataset_overrides=sandbox_dataset_overrides,
                # If a given view hasn't been loaded to the sandbox, skip it
                skip_missing_views=True,
            )
        else:
            create_dataset_and_deploy_views_for_view_builders(
                bq_view_namespace=namespace,
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                view_builders_to_update=builders_to_update,
                dataset_overrides=sandbox_dataset_overrides,
            )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="A prefix to append to all names of the datasets where these views will be loaded.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--dataflow_dataset_override",
        dest="dataflow_dataset_override",
        help="An override of the dataset containing Dataflow metric output that the updated "
        "views should reference.",
        type=str,
        required=False,
    )

    parser.add_argument(
        "--refresh_materialized_tables_only",
        dest="refresh_materialized_tables_only",
        help="If True, will only rematerialize views that have already been loaded to the sandbox.",
        type=str_to_bool,
        default=False,
        required=False,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        logging.info(
            "Prefixing all view datasets with [%s_].", known_args.sandbox_dataset_prefix
        )

        load_views_to_sandbox(
            known_args.sandbox_dataset_prefix,
            known_args.dataflow_dataset_override,
            known_args.refresh_materialized_tables_only,
        )
