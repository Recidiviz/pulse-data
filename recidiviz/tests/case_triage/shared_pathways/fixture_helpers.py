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
"""Helper to load metrics fixtures for Pathways and Public Pathways tests."""
import csv
import os
from typing import Dict, List

from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema.public_pathways.schema import (
    PublicPathwaysBase,
)
from recidiviz.persistence.database.schema_utils import ModuleType


def load_metrics_fixture(
    model: PathwaysBase | PublicPathwaysBase,
    fixtures: ModuleType,
    filename: str | None = None,
) -> List[Dict]:
    filename_str = f"{model.__tablename__}.csv" if filename is None else filename
    if fixtures.__file__ is None:
        raise RuntimeError(
            f"Cannot load fixture '{filename_str}': "
            f"fixtures module {fixtures.__name__} has no __file__ attribute. "
            "Ensure the fixtures module is a regular Python module with a file path."
        )
    fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename_str)
    if not os.path.exists(fixture_path):
        raise FileNotFoundError(
            f"Fixture file not found: {fixture_path}. "
            f"Ensure the CSV file '{filename_str}' exists in the fixtures directory."
        )
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            results.append(row)

    return results
