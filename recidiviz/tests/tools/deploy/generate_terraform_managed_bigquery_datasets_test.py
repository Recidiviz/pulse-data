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
"""Tests that terraform_managed_bigquery_datasets.yaml has been freshly
regenerated."""
import os
import re
import unittest
from collections import defaultdict

from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.externally_managed.datasets import (
    MANUALLY_UPDATED_SOURCE_TABLES_DATASET,
)
from recidiviz.tools import deploy
from recidiviz.tools.deploy.generate_terraform_managed_bigquery_datasets import (
    _NON_SOURCE_TABLE_TERRAFORM_MANAGED_DATASETS,
    TERRAFORM_MANAGED_BIGQUERY_DATASETS_YAML_PATH,
    build_registry_contents,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override

# TODO(OBT-45863): Remove once experiment_assignments_large migrates out of
# manually_updated_source_tables and the dataset leaves the source-table
# repository.
_SOURCE_TABLE_REPOSITORY_DATASET_EXCEPTIONS = {MANUALLY_UPDATED_SOURCE_TABLES_DATASET}


class GenerateTerraformManagedBigqueryDatasetsTest(unittest.TestCase):
    """Tests that terraform_managed_bigquery_datasets.yaml has been freshly
    regenerated."""

    def test_yaml_is_freshly_generated(self) -> None:
        """Regenerates the registry contents and asserts the checked-in file
        matches, so the datasets Terraform creates never drift from the
        datasets the source-table collections generate."""
        with open(
            TERRAFORM_MANAGED_BIGQUERY_DATASETS_YAML_PATH, encoding="utf-8"
        ) as yaml_file:
            checked_in_contents = yaml_file.read()

        self.assertEqual(
            build_registry_contents(),
            checked_in_contents,
            "terraform_managed_bigquery_datasets.yaml is stale. Re-generate it "
            "by running `uv run python -m "
            "recidiviz.tools.deploy.generate_terraform_managed_bigquery_datasets`.",
        )

    def test_non_source_table_datasets_not_in_source_table_repository(self) -> None:
        """The hand-list exists for datasets the source-table framework knows
        nothing about. A dataset registered in the SourceTableRepository is
        either registered in the Terraform registry automatically by the
        generator (when the framework manages its tables) or deliberately left
        out of Terraform entirely (when an external system owns it), so a
        hand-list entry for one is a duplicate or a contradiction.
        """
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with local_project_id_override(project_id):
                repository = build_source_table_repository_for_collected_schemata(
                    project_id=project_id
                )
            source_table_dataset_ids = {
                collection.dataset_id
                for collection in repository.source_table_collections
            }
            overlap = (
                set(_NON_SOURCE_TABLE_TERRAFORM_MANAGED_DATASETS)
                & source_table_dataset_ids
            ) - _SOURCE_TABLE_REPOSITORY_DATASET_EXCEPTIONS
            self.assertFalse(
                overlap,
                f"Datasets declared in _NON_SOURCE_TABLE_TERRAFORM_MANAGED_DATASETS "
                f"are also registered in the SourceTableRepository for "
                f"[{project_id}]: {sorted(overlap)}. If the source-table "
                f"framework manages the dataset's tables, remove the explicit "
                f"declaration; the generator registers the dataset "
                f"automatically. If an external system manages the tables, the "
                f"dataset should not be declared in Terraform at all.",
            )

    def test_no_dataset_declarations_outside_the_registry(self) -> None:
        """A BigQuery dataset created by Terraform should come from the registry
        (or the hand-list feeding it), not from a standalone declaration in
        another .tf file, which would bypass registration entirely. This scans
        every .tf file for dataset declarations and pins the exact set of
        allowed declaration sites.
        """
        terraform_root = os.path.join(os.path.dirname(deploy.__file__), "terraform")
        declaration_patterns = {
            "resource": re.compile(
                r'^resource\s+"google_bigquery_dataset"', re.MULTILINE
            ),
            "module": re.compile(r'source\s*=\s*"[^"]*/big_query_dataset"'),
        }
        declaration_counts: dict[tuple[str, str], int] = defaultdict(int)
        for dirpath, _, filenames in os.walk(terraform_root):
            for filename in filenames:
                if not filename.endswith(".tf"):
                    continue
                path = os.path.join(dirpath, filename)
                with open(path, encoding="utf-8") as tf_file:
                    contents = tf_file.read()
                relative_path = os.path.relpath(path, terraform_root)
                for kind, pattern in declaration_patterns.items():
                    if count := len(pattern.findall(contents)):
                        declaration_counts[(relative_path, kind)] += count

        self.assertEqual(
            {
                # The registry.
                ("bigquery-datasets.tf", "module"): 1,
                # The dataset module's underlying resource.
                ("modules/big_query_dataset/main.tf", "resource"): 1,
                # Per-state scratch and validation-oneoff datasets.
                ("modules/state-direct-ingest-resources/bigquery.tf", "module"): 2,
                # The oncall logs dataset, populated by a Cloud Logging sink.
                ("cloud-logging.tf", "resource"): 1,
            },
            dict(declaration_counts),
            "Found an unexpected BigQuery dataset declaration in the Terraform "
            "config. Create datasets via the registry instead: register a "
            "source-table collection (the generator picks its dataset up "
            "automatically) or add the dataset to "
            "_NON_SOURCE_TABLE_TERRAFORM_MANAGED_DATASETS. If a new standalone "
            "declaration site is genuinely needed, update the expected counts "
            "here.",
        )
