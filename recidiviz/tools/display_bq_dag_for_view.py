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

from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager import (
    _build_views_to_update,
)
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_VIEW_BUILDERS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


def build_dag_walker(dataset_id: str, view_id: str) -> BigQueryViewDagWalker:
    is_valid_view = False
    for builder in DEPLOYED_VIEW_BUILDERS:
        if (
            not is_valid_view
            and builder.dataset_id == dataset_id
            and builder.view_id == view_id
        ):
            is_valid_view = True
    if not is_valid_view:
        raise ValueError(f"invalid view {dataset_id}.{view_id}")
    return BigQueryViewDagWalker(
        _build_views_to_update(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            candidate_view_builders=DEPLOYED_VIEW_BUILDERS,
            dataset_overrides=None,
        )
    )


def print_dfs_tree(dataset_id: str, view_id: str) -> None:
    dag_walker = build_dag_walker(dataset_id, view_id)
    print(
        dag_walker.get_dfs_tree_str_for_table(
            dataset_id=dataset_id,
            table_id=view_id,
        )
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
    args = parser.parse_args()

    with local_project_id_override(args.project_id):
        print_dfs_tree(args.dataset_id, args.view_id)
