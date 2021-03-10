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
"""Implements useful helper functions for Case Triage."""
import os
from typing import Dict, List

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema_utils import get_case_triage_table_classes
from recidiviz.utils import metadata


def get_importable_csvs() -> Dict[str, GcsfsFilePath]:
    """Returns a map from view ids to GcsfsFilePaths where the views have
    been exported to CSVs."""
    gcsfs = GcsfsFactory.build()

    files_in_import_folder = [
        f
        for f in gcsfs.ls_with_blob_prefix(
            bucket_name=f"{metadata.project_id()}-case-triage-data",
            blob_prefix="to_import",
        )
        if isinstance(f, GcsfsFilePath)
    ]

    importable_csvs = {}

    for f in files_in_import_folder:
        root, ext = os.path.splitext(f.file_name)
        if ext != ".csv":
            continue
        importable_csvs[root] = f

    return importable_csvs


def columns_for_case_triage_view(view_id: str) -> List[str]:
    for table in get_case_triage_table_classes():
        if table.name == view_id:
            return [col.name for col in table.columns]

    raise ValueError(f"No table found for view_id: {view_id}")
