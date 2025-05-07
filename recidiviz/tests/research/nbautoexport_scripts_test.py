# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
Unit tests for the export_notebooks_pre_commit module.
"""
import unittest
from pathlib import Path


class TestNbAutoexportScriptsSyncing(unittest.TestCase):
    """
    Test suite to validate that all notebooks in recidiviz/research have a corresponding
    nbautoexport-scripts directory and that all scripts in those directories correspond
    to the notebooks.
    """

    def test_every_notebook_has_exported_script(self) -> None:
        # Locate the recidiviz/research folder from this test file
        repo_root = Path(__file__).parents[2]
        research_dir = repo_root / "research"
        # Walk all .ipynb files under recidiviz/research
        for nb_path in research_dir.rglob("*.ipynb"):
            # Skip notebooks inside nbautoexport-scripts dirs, if any
            if "nbautoexport-scripts" in nb_path.parts:
                continue
            with self.subTest(notebook=str(nb_path)):
                scripts_dir = nb_path.parent / "nbautoexport-scripts"
                self.assertTrue(
                    scripts_dir.is_dir(),
                    f"Missing directory {scripts_dir} for notebook {nb_path}",
                )

                expected_py = scripts_dir / (nb_path.stem + ".py")
                self.assertTrue(
                    expected_py.exists(),
                    f"Missing exported script {expected_py} for notebook {nb_path}",
                )

    def test_exported_scripts_correspond_to_notebooks(self) -> None:
        # Locate the recidiviz/research folder from this test file
        repo_root = Path(__file__).parents[2]
        research_dir = repo_root / "research"

        # For every .py in nbautoexport-scripts (except __init__.py),
        # verify a notebook of the same stem exists in the parent dir.
        for script_path in research_dir.rglob("nbautoexport-scripts/*.py"):
            if script_path.name == "__init__.py":
                continue
            with self.subTest(script=str(script_path)):
                notebook_path = script_path.parent.parent / (
                    script_path.stem + ".ipynb"
                )
                self.assertTrue(
                    notebook_path.exists(),
                    f"Missing notebook {notebook_path} for exported script {script_path}",
                )


if __name__ == "__main__":
    unittest.main()
