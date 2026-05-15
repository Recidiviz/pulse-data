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
"""Schema enforcement test: asserts that the BQ eval source table YAML and
golden_eval_config.yaml stay in sync with each other and with collection.yaml.

This test runs in standard CI (no LLM calls or BQ access required).  If it
fails, re-run generate_eval_source_table_yamls.py to regenerate the BQ yamls.
"""
import os
import unittest

import recidiviz.NOT_FOR_PRODUCTION_USE.source_tables as _nfpu_source_tables
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    collect_extractor_collection_eval_configs,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.source_tables.generate_eval_source_table_yamls import (
    generate_eval_source_table_yaml,
)

_EVAL_YAMLS_DIR = os.path.join(
    os.path.dirname(_nfpu_source_tables.__file__),
    "yaml_managed",
    "document_extraction_collection_golden_eval_data",
)


class EvalSchemaEnforcementTest(unittest.TestCase):
    """Asserts that golden_eval_config.yaml and the BQ source table YAML stay in sync."""

    def test_committed_eval_source_table_yamls_are_up_to_date(self) -> None:
        """Asserts that generate_eval_source_table_yamls.py has been re-run after
        any schema change.

        Regenerates each collection's YAML content in memory and compares it to
        the committed file.  If this fails, re-run generate_eval_source_table_yamls.py.
        """
        eval_configs = collect_extractor_collection_eval_configs()

        for collection_name, eval_schema in sorted(eval_configs.items()):
            with self.subTest(collection_name=collection_name):
                expected_content = generate_eval_source_table_yaml(
                    collection_name=collection_name,
                    eval_schema=eval_schema,
                )

                committed_path = os.path.join(
                    _EVAL_YAMLS_DIR, f"{collection_name.lower()}.yaml"
                )
                self.assertTrue(
                    os.path.exists(committed_path),
                    msg=(
                        f"No committed BQ source table yaml found for "
                        f"{collection_name} at {committed_path}. "
                        f"Re-run generate_eval_source_table_yamls.py."
                    ),
                )

                with open(committed_path, encoding="utf-8") as f:
                    committed_content = f.read()

                self.assertEqual(
                    expected_content,
                    committed_content,
                    msg=(
                        f"Committed BQ source table yaml for {collection_name} is "
                        f"stale. Re-run generate_eval_source_table_yamls.py."
                    ),
                )


if __name__ == "__main__":
    unittest.main()
