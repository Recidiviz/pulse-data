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
"""Tests that the generated raw table YAMLs are in sync with the task configs."""
import os
import tempfile
import unittest

import yaml

import recidiviz.source_tables.yaml_managed as _yaml_managed_pkg
from recidiviz.llm_eval.label_studio.models.label_studio_project_config import (
    collect_label_studio_project_configs,
)
from recidiviz.tools.llm_eval.label_studio.generate_raw_table_yamls import (
    generate_raw_table_yamls,
)

_COMMITTED_DIR = os.path.join(
    os.path.dirname(_yaml_managed_pkg.__file__),
    "gcs_backed_tables",
    "llm_eval_label_studio",
)


class GenerateRawTableYamlsTest(unittest.TestCase):
    """Verifies generated YAMLs match the committed files."""

    def test_generated_yamls_are_up_to_date(self) -> None:
        """Fails if committed YAML files are out of sync with task configs.

        If this test fails, re-run:
            python -m recidiviz.tools.llm_eval.label_studio.generate_raw_table_yamls
        and commit the updated files.
        """
        configs = collect_label_studio_project_configs()
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raw_table_yamls(output_dir=tmpdir)
            for config in configs.values():
                filename = f"{config.raw_table_id}.yaml"
                generated_path = os.path.join(tmpdir, filename)
                committed_path = os.path.join(_COMMITTED_DIR, filename)

                with open(generated_path, encoding="utf-8") as f:
                    generated = yaml.safe_load(f)

                self.assertTrue(
                    os.path.exists(committed_path),
                    msg=(
                        f"Missing committed YAML [{committed_path}]. "
                        f"Run: python -m recidiviz.tools.llm_eval.label_studio.generate_raw_table_yamls"
                    ),
                )
                with open(committed_path, encoding="utf-8") as f:
                    committed = yaml.safe_load(f)

                self.assertEqual(
                    generated,
                    committed,
                    msg=(
                        f"Committed YAML [{filename}] is out of sync with the task config. "
                        f"Run: python -m recidiviz.tools.llm_eval.label_studio.generate_raw_table_yamls"
                    ),
                )

    def test_no_extra_committed_yamls(self) -> None:
        """Fails if there are committed raw table YAMLs with no corresponding task config."""
        configs = collect_label_studio_project_configs()
        expected_filenames = {
            f"{config.raw_table_id}.yaml" for config in configs.values()
        }
        committed_yaml_files = {
            name
            for name in os.listdir(_COMMITTED_DIR)
            if name.endswith("_annotations_raw.yaml")
        }
        extra = committed_yaml_files - expected_filenames
        self.assertFalse(
            extra,
            msg=(
                f"Found committed raw table YAML(s) with no corresponding task config: {extra}. "
                f"Delete them or add a matching task config."
            ),
        )
