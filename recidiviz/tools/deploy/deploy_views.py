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
from typing import List, Tuple

from recidiviz.big_query.view_update_manager import (
    copy_dataset_schemas_to_sandbox,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
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

    return parser.parse_known_args(argv)


def deploy_views(project_id: str, test_schema: bool) -> None:
    """Deploys the views"""
    view_builders = deployed_view_builders(project_id)
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
            view_builders=view_builders,
            override_source_datasets=True,
        )

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        view_builders_to_update=view_builders,
        dataset_overrides=test_dataset_overrides,
        historically_managed_datasets_to_clean=DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
        default_table_expiration_for_new_datasets=DEFAULT_TEMPORARY_TABLE_EXPIRATION,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        deploy_views(known_args.project_id, known_args.test_schema_only)
