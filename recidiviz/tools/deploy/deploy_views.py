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
Script for deploying updated views to BigQuery. Should not be run outside the context of a deploy.
Run locally with the following command:
    python -m recidiviz.big_query.view_update_manager --project_id [PROJECT_ID]
"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.view_update_manager import (
    create_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.view_registry.deployed_views import DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


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

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        for namespace, builders in DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE.items():
            # TODO(#5785): Clarify use case of BigQueryViewNamespace filter (see ticket for more)
            create_dataset_and_deploy_views_for_view_builders(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                bq_view_namespace=namespace,
                view_builders_to_update=builders,
            )
