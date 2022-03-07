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
"""
Script for deploying updated views to BigQuery. Should not be run outside the context
of a deploy.

Run locally with the following command:
    python -m recidiviz.tools.deploy.deploy_views --project-id [PROJECT_ID]

To test the schemas by deploying to an empty sandbox, run:
    python -m recidiviz.tools.deploy.deploy_views --project-id [PROJECT_ID] --test-schema-only

"""
import argparse
import logging
import sys
import uuid
from typing import List, Optional, Sequence, Tuple

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.view_update_manager import (
    copy_dataset_schemas_to_sandbox,
    create_managed_dataset_and_deploy_views_for_view_builders,
    view_builder_sub_graph_for_view_builders_to_load,
)
from recidiviz.tools.load_views_to_sandbox import str_to_list
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.dataset_overrides import (
    dataset_overrides_for_view_builders,
)
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
    deployed_view_builders,
)

# If deploying the views against empty source tables starts to take over an hour, this
# will break. We set this to a short period of time because it creates a *lot* of
# datasets that clutter BigQuery.
DEFAULT_TEMPORARY_TABLE_EXPIRATION = 60 * 60 * 1000  # 1 hour


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--test-schema-only",
        type=bool,
        nargs="?",
        const=True,
        default=False,
        help="If set, copies the schemas of the source tables to a sandbox and deploys "
        "all views on top of those to test for query compilation errors without "
        "materializing all of the views.",
    )

    parser.add_argument(
        "--dataset-ids-to-load",
        dest="dataset_ids_to_load",
        help="A list of dataset_ids to load separated by commas. If provided, only "
        "loads datasets in this list plus ancestors.",
        type=str_to_list,
        required=False,
    )

    return parser.parse_known_args(argv)


def deploy_views(
    project_id: str,
    test_schema: bool,
    dataset_ids_to_load: Optional[List[str]] = None,
) -> None:
    """Deploys the views.

    If |test_schema| is True, deploys a temporary "test" version of all views on top
    of empty source tables to test for query compilation errors.

    If |dataset_ids_to_load| is set, only loads views whose dataset_id matches one of
    the listed dataset_ids, or views that are ancestors of the views in the datasets
    listed.
    """
    view_builders_to_update: Sequence[BigQueryViewBuilder] = deployed_view_builders(
        project_id
    )

    if dataset_ids_to_load:
        logging.info(
            "Limiting view deploy to views in datasets %s and any view ancestors.",
            dataset_ids_to_load,
        )
        all_view_builders_in_dag = view_builders_to_update
        view_builders_in_datasets = [
            view
            for view in all_view_builders_in_dag
            if view.dataset_id in dataset_ids_to_load
        ]
        view_builders_to_update = view_builder_sub_graph_for_view_builders_to_load(
            view_builders_to_load=view_builders_in_datasets,
            all_view_builders_in_dag=view_builders_in_datasets,
            get_ancestors=True,
            get_descendants=False,
        )

    test_dataset_overrides = None

    if test_schema:
        test_dataset_prefix = f"deploy_{str(uuid.uuid4())[:6]}"
        logging.info("Creating view tree with prefix: '%s'", test_dataset_prefix)

        copy_dataset_schemas_to_sandbox(
            datasets=VIEW_SOURCE_TABLE_DATASETS,
            sandbox_prefix=test_dataset_prefix,
            default_table_expiration=DEFAULT_TEMPORARY_TABLE_EXPIRATION,
        )

        test_dataset_overrides = dataset_overrides_for_view_builders(
            view_dataset_override_prefix=test_dataset_prefix,
            view_builders=view_builders_to_update,
            override_source_datasets=True,
        )

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        view_builders_to_update=view_builders_to_update,
        dataset_overrides=test_dataset_overrides,
        historically_managed_datasets_to_clean=DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
        default_table_expiration_for_new_datasets=DEFAULT_TEMPORARY_TABLE_EXPIRATION,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        deploy_views(
            known_args.project_id,
            known_args.test_schema_only,
            known_args.dataset_ids_to_load,
        )
