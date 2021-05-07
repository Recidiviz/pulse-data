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
"""GCS Store used to keep counts of column values across datasets whose data is organized by state."""

import json
import logging
import os
from collections import defaultdict
from typing import Dict, Optional

import attr

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.cloud_storage.gcs_file_system import GCSBlobDoesNotExistError
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


@attr.s
class DatasetMetadataCounts:
    total_count: int = attr.ib()
    placeholder_count: Optional[int] = attr.ib(default=None)

    def to_json(self) -> Dict[str, int]:
        base_dict = {
            "totalCount": self.total_count,
        }
        if self.placeholder_count is not None:
            base_dict["placeholderCount"] = self.placeholder_count
        return base_dict

    @staticmethod
    def from_json(json_dict: Dict[str, str]) -> "DatasetMetadataCounts":
        total_count = int(json_dict["total_count"])
        placeholder_count = (
            None
            if "placeholder_count" not in json_dict
            else int(json_dict["placeholder_count"])
        )
        return DatasetMetadataCounts(
            total_count=total_count,
            placeholder_count=placeholder_count,
        )


# Maps from name of result (e.g. column name or table name) to state codes to a
# dict of CountKeys to actual counts.
DatasetMetadataResult = Dict[str, Dict[str, DatasetMetadataCounts]]

# An InternalMetadataCountsStore maps from table -> column name -> column value -> state_code -> counts
InternalMetadataBackingStore = Dict[
    str, Dict[str, Dict[str, Dict[str, DatasetMetadataCounts]]]
]


class DatasetMetadataCountsStore(AdminPanelStore):
    """Creates a store for fetching counts of different column values among tables in some dataset from GCS."""

    def __init__(
        self,
        dataset_nickname: str,
        metadata_file_prefix: str,
        override_project_id: Optional[str] = None,
    ) -> None:
        super().__init__(override_project_id)
        self.gcs_fs = GcsfsFactory.build()
        self.dataset_nickname = dataset_nickname
        self.metadata_file_prefix = metadata_file_prefix

        # This class takes heavy advantage of the fact that python dicts are thread-safe.
        self.store: InternalMetadataBackingStore = defaultdict(
            lambda: defaultdict(dict)
        )

    def recalculate_store(self) -> None:
        """Recalculates the internal store of dataset metadata counts."""
        file_paths = [
            f
            for f in self.gcs_fs.ls_with_blob_prefix(
                f"{self.project_id}-{self.dataset_nickname}-metadata", ""
            )
            if isinstance(f, GcsfsFilePath)
        ]
        for path in file_paths:
            name, extension = os.path.splitext(path.file_name)
            if extension != ".json":
                logging.warning(
                    "Found unexpected file in %s metadata folder: %s",
                    self.dataset_nickname,
                    path.file_name,
                )
                continue
            try:
                file_prefix, table_name, col_name = name.split("__")
            except ValueError:
                # There will be files in this directory that don't follow this structure, so it's safe
                # to skip them without alarm.
                continue
            if file_prefix != self.metadata_file_prefix:
                logging.warning(
                    "Found unexpected file in %s metadata folder: %s",
                    self.dataset_nickname,
                    path.file_name,
                )
                continue

            logging.debug(
                "Processing %s file to retrieve %s metadata",
                path.file_name,
                self.dataset_nickname,
            )

            col_store: Dict[str, Dict[str, DatasetMetadataCounts]] = defaultdict(dict)

            try:
                result = self.gcs_fs.download_as_string(path)
            except GCSBlobDoesNotExistError:
                continue
            lines = result.split("\n")
            for l in lines:
                l = l.strip()
                if not l:
                    continue
                struct = json.loads(l)
                col_store[struct[col_name]][
                    struct["state_code"].upper()
                ] = DatasetMetadataCounts.from_json(struct)
            self.store[table_name][col_name] = col_store

        logging.debug("DONE PROCESSING FOR %s METADATA", self.dataset_nickname)

    def fetch_object_counts_by_table(self) -> DatasetMetadataResult:
        """
        This method calculates the total number of observed entities (both placeholder and not) per table
        by picking the counts from the column with the greatest number of entities.
        """
        results: DatasetMetadataResult = defaultdict(dict)
        for table in self.store:
            results[table] = {}
            max_state_placeholder: Dict[str, int] = defaultdict(lambda: -1)
            max_state_total: Dict[str, int] = defaultdict(lambda: -1)

            # Since the primary key is always nonnull, the max for each table will also represent
            # the number of total objects in that table.
            breakdown_by_column = self.fetch_table_nonnull_counts_by_column(table)
            for state_map in breakdown_by_column.values():
                for state_code, result in state_map.items():
                    if result.total_count > max_state_total[state_code]:
                        max_state_total[state_code] = result.total_count
                    if (
                        result.placeholder_count is not None
                        and result.placeholder_count > max_state_placeholder[state_code]
                    ):
                        max_state_placeholder[state_code] = result.placeholder_count
            for state_code in max_state_placeholder.keys():
                state_placeholder: Optional[int] = None
                if max_state_placeholder[state_code] >= 0:
                    state_placeholder = max_state_placeholder[state_code]
                results[table][state_code] = DatasetMetadataCounts(
                    total_count=max_state_total[state_code],
                    placeholder_count=state_placeholder,
                )
        return results

    def fetch_table_nonnull_counts_by_column(self, table: str) -> DatasetMetadataResult:
        """
        This code does the equivalent of:
        `SELECT state_code, col, SUM(total_count) AS total_count, SUM(placeholder_count) AS placeholder_count
        FROM column_metadata WHERE table_name=$1 AND value IS NOT NULL GROUP BY state_code, col`
        """
        if table not in self.store:
            return {}

        results: DatasetMetadataResult = defaultdict(dict)
        for col, val_map in self.store[table].items():
            has_placeholders = False
            placeholder_count: Dict[str, int] = defaultdict(int)
            total_count: Dict[str, int] = defaultdict(int)
            for val, state_map in val_map.items():
                if val == "NULL":
                    continue
                for state_code, result in state_map.items():
                    total_count[state_code] += result.total_count
                    if result.placeholder_count is not None:
                        has_placeholders = True
                        placeholder_count[state_code] += result.placeholder_count
            for state_code in placeholder_count.keys():
                results[col][state_code] = DatasetMetadataCounts(
                    total_count=total_count[state_code],
                    placeholder_count=(
                        placeholder_count[state_code] if has_placeholders else None
                    ),
                )

        return results

    def fetch_column_object_counts_by_value(
        self, table: str, column: str
    ) -> DatasetMetadataResult:
        """
        This code does the equivalent of:
        SELECT state_code, val, total_count, placeholder_count FROM column_metadata WHERE table_name=$1 AND col=$2
        """
        if table not in self.store or column not in self.store[table]:
            return {}
        return self.store[table][column]
