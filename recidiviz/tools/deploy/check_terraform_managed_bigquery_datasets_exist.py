# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Checks that every Terraform-registered BigQuery dataset already exists in a
project, before the one-time adoption import in bigquery-datasets.tf runs during
that project's deploy.

TODO(OBT-44639): Delete this script together with the one-time import block once
both environments have adopted the registered datasets.

The import block adopts the datasets that already exist in BigQuery into
Terraform state instead of creating them. An import errors on a dataset that
does not exist yet, so run this against a project right before the release
carrying the registry deploys there and confirm it reports nothing missing. Any
dataset the check reports must be created, or removed from the registry, before
the deploy.

Only dataset existence is read (via list_datasets), so no table data is
accessed.

    uv run python -m recidiviz.tools.deploy.check_terraform_managed_bigquery_datasets_exist \
        --project recidiviz-staging
"""
import argparse
import sys

import yaml

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.tools.deploy.generate_terraform_managed_bigquery_datasets import (
    TERRAFORM_MANAGED_BIGQUERY_DATASETS_YAML_PATH,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def registered_datasets_for_project(project_id: str) -> set[str]:
    """Returns the registered dataset ids that apply to the given project,
    mirroring the project filter in bigquery-datasets.tf: a dataset with no
    projects key applies to every project, otherwise only to the listed ones."""
    with open(
        TERRAFORM_MANAGED_BIGQUERY_DATASETS_YAML_PATH, encoding="utf-8"
    ) as registry_file:
        registry = yaml.safe_load(registry_file)
    return {
        dataset_id
        for dataset_id, config in registry["datasets"].items()
        if config.get("projects") is None or project_id in config["projects"]
    }


def missing_registered_datasets(project_id: str) -> list[str]:
    """Returns the registered datasets that do not yet exist in the project,
    sorted. An empty list means the adoption import will not error there."""
    registered = registered_datasets_for_project(project_id)
    existing = {dataset.dataset_id for dataset in BigQueryClientImpl().list_datasets()}
    return sorted(registered - existing)


def main(project_id: str) -> int:
    """Prints the registered datasets missing from the project and returns a
    non-zero exit code when any are missing."""
    missing = missing_registered_datasets(project_id)
    if missing:
        print(
            f"[{project_id}] {len(missing)} registered dataset(s) do not exist "
            f"in BigQuery. The adoption import would error on these; create them "
            f"or remove them from the registry before deploying:"
        )
        for dataset_id in missing:
            print(f"  - {dataset_id}")
        return 1

    registered_count = len(registered_datasets_for_project(project_id))
    print(
        f"[{project_id}] OK: all {registered_count} registered datasets exist in "
        f"BigQuery. The adoption import will not error."
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        dest="project_id",
        required=True,
        choices=DATA_PLATFORM_GCP_PROJECTS,
        help="The GCP project to check registered datasets against.",
    )
    args = parser.parse_args()
    with local_project_id_override(args.project_id):
        sys.exit(main(args.project_id))
