# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Script for deleting any unmanaged views and datasets within a BigQuery project.

Note that this only deletes unmanaged views and datasets from datasets that have ever
been regularly updated by our deploy process (see
DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED). This does not delete any views or
datasets created during the federated CloudSQL to BQ refresh process.

When run in dry-run mode (the default), will only log the unmanaged tables/datasets to
be deleted, but will not actually delete them

Example terminal execution:
python -m recidiviz.tools.run_delete_unmanaged_views --project-id recidiviz-staging --dry-run True
"""
import argparse
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryViewBuilderShouldNotBuildError
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager_utils import (
    cleanup_datasets_and_delete_unmanaged_views,
    get_managed_view_and_materialized_table_addresses_by_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id
from recidiviz.utils.params import str_to_bool
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
    deployed_view_builders,
)


def main() -> None:
    """Executes the main flow of the script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project against which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs delete in dry-run mode, only prints the views/tables it would delete.",
    )
    args = parser.parse_args()
    logging.getLogger().setLevel(logging.INFO)

    with local_project_id_override(args.project_id):
        views = []
        for view_builder in deployed_view_builders(project_id()):
            if view_builder.dataset_id in VIEW_SOURCE_TABLE_DATASETS:
                raise ValueError(
                    f"Found view [{view_builder.view_id}] in source-table-only dataset [{view_builder.dataset_id}]"
                )

            try:
                view = view_builder.build()
            except BigQueryViewBuilderShouldNotBuildError:
                logging.warning(
                    "Condition failed for view builder %s in dataset %s. Continuing without it.",
                    view_builder.view_id,
                    view_builder.dataset_id,
                )
                continue
            views.append(view)

        dag_walker = BigQueryViewDagWalker(views)
        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        cleanup_datasets_and_delete_unmanaged_views(
            bq_client=BigQueryClientImpl(),
            managed_views_map=managed_views_map,
            dry_run=args.dry_run,
            datasets_that_have_ever_been_managed=DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
        )


if __name__ == "__main__":
    main()
