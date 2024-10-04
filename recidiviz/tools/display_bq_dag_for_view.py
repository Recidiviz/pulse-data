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
"""A script for displaying the dependency tree of a given BigQuery view, including parent tables.

Prints this directly to stdout.

python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id po_report_views --view_id po_monthly_report_data
"""
import argparse
import logging

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.build_views_to_update import build_views_to_update
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_datasets,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.view_registry.deployed_views import all_deployed_view_builders


def print_dfs_tree(
    dataset_id: str, view_id: str, print_downstream_tree: bool = False
) -> None:

    all_source_datasets = get_all_source_table_datasets()
    all_view_builders = all_deployed_view_builders()

    # It doesn't matter what the project_id is - we're just building the graph to
    # understand view relationships
    with local_project_id_override(GCP_PROJECT_STAGING):
        views = build_views_to_update(
            view_source_table_datasets=all_source_datasets,
            candidate_view_builders=all_view_builders,
            sandbox_context=None,
        )

    dag_walker = BigQueryViewDagWalker(views)

    address = BigQueryAddress(dataset_id=dataset_id, table_id=view_id)
    if address not in dag_walker.nodes_by_address:
        raise ValueError(f"invalid view {address.to_str()}")

    if print_downstream_tree:
        dag_walker.populate_descendant_sub_dags()
    else:
        dag_walker.populate_ancestor_sub_dags()
    view = dag_walker.view_for_address(
        view_address=BigQueryAddress(dataset_id=dataset_id, table_id=view_id)
    )
    print(
        dag_walker.descendants_dfs_tree_str(view=view)
        if print_downstream_tree
        else dag_walker.ancestors_dfs_tree_str(view=view)
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        required=True,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="The project_id where the existing view lives",
    )
    parser.add_argument(
        "--dataset_id",
        required=True,
        help="The name of the dataset containing the view (e.g. po_report_views)",
    )
    parser.add_argument(
        "--view_id", required=True, help="The name of the view (po_monthly_report_data)"
    )
    parser.add_argument(
        "--show_downstream_dependencies",
        required=False,
        default=False,
        type=str_to_bool,
        help="If True, displays the downstream DAG graph instead of the upstream graph.",
    )
    args = parser.parse_args()

    with local_project_id_override(args.project_id):
        print_dfs_tree(args.dataset_id, args.view_id, args.show_downstream_dependencies)
