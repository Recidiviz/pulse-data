# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for datasets.py."""
import unittest

from recidiviz.view_registry.datasets import (
    VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS,
    VIEW_SOURCE_TABLE_DATASETS,
)


class DatasetsTest(unittest.TestCase):
    """Tests for references in datasets.py."""

    def test_view_source_table_datasets_to_descriptions(self) -> None:
        for dataset in VIEW_SOURCE_TABLE_DATASETS:
            self.assertIn(dataset, VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS.keys())
