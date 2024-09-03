# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for recidiviz/source_tables/externally_managed/datasets.py"""
import unittest

from recidiviz.source_tables.collect_all_source_table_configs import (
    collect_externally_managed_source_table_collections,
)
from recidiviz.source_tables.externally_managed.datasets import (
    EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS,
)


class TestExternallyManagedDatasets(unittest.TestCase):
    """Tests for recidiviz/source_tables/externally_managed/datasets.py"""

    def test_externally_managed_datasets_to_descriptions(self) -> None:
        datasets_with_descriptions = set(EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS)

        externally_managed_collections = (
            collect_externally_managed_source_table_collections(project_id=None)
        )

        datasets_in_externally_managed_collections = {
            c.dataset_id for c in externally_managed_collections
        }

        extra_datasets = (
            datasets_with_descriptions - datasets_in_externally_managed_collections
        )

        if extra_datasets:
            extra_datasets_str = "\n".join(f" * {d}" for d in extra_datasets)
            raise ValueError(
                f"Found datasets defined in "
                f"EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS which are not actually "
                f"part of a source table collection. Either add definitions for tables "
                f"in these datasets to recidiviz/source_tables/externally_managed/ or "
                f"remove from EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS. Extra "
                f"datasets:\n{extra_datasets_str}"
            )
