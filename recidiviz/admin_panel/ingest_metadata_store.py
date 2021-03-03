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
"""GCS Store used to keep counts of column values across state database."""
import json
import logging
import os
from collections import defaultdict
from typing import Dict, List, Optional, Union

import attr

from recidiviz.cloud_storage.gcs_file_system import GCSBlobDoesNotExistError
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType
from recidiviz.utils import metadata


@attr.s
class IngestMetadataCounts:
    total_count: int = attr.ib()
    placeholder_count: int = attr.ib()

    def to_json(self) -> Dict[str, int]:
        return {
            "totalCount": self.total_count,
            "placeholderCount": self.placeholder_count,
        }

    @staticmethod
    def from_json(json_dict: Dict[str, str]) -> "IngestMetadataCounts":
        return IngestMetadataCounts(
            total_count=int(json_dict["total_count"]),
            placeholder_count=int(json_dict["placeholder_count"]),
        )


# Maps from name of result (e.g. column name or table name) to state codes to a
# dict of CountKeys to actual counts.
IngestMetadataResult = Dict[str, Dict[str, IngestMetadataCounts]]

# An InternalMetadataCountsStore maps from table -> column name -> column value -> state_code -> counts
InternalMetadataBackingStore = Dict[
    str, Dict[str, Dict[str, Dict[str, IngestMetadataCounts]]]
]


class IngestMetadataCountsStore:
    """Creates a store for fetching counts of different column values from GCS."""

    def __init__(self, override_project_id: Optional[str] = None) -> None:
        self.gcs_fs = GcsfsFactory.build()
        self.override_project_id = override_project_id

        self.data_freshness_results: List[Dict[str, Union[str, bool]]] = []

        # This class takes heavy advantage of the fact that python dicts are thread-safe.
        self.store: InternalMetadataBackingStore = defaultdict(
            lambda: defaultdict(dict)
        )

    @property
    def project_id(self) -> str:
        return (
            metadata.project_id()
            if self.override_project_id is None
            else self.override_project_id
        )

    def recalculate_store(self) -> None:
        """
        Recalculates the internal store of ingest metadata counts.
        """
        self.update_data_freshness_results()

        file_paths = [
            f
            for f in self.gcs_fs.ls_with_blob_prefix(
                f"{self.project_id}-ingest-metadata", ""
            )
            if isinstance(f, GcsfsFilePath)
        ]
        for path in file_paths:
            name, extension = os.path.splitext(path.file_name)
            if extension != ".json":
                logging.warning(
                    "Found unexpected file in ingest metadata folder: %s",
                    path.file_name,
                )
                continue
            try:
                file_prefix, table_name, col_name = name.split("__")
            except ValueError:
                # There will be files in this directory that don't follow this structure, so it's safe
                # to skip them without alarm.
                continue
            if file_prefix != "ingest_state_metadata":
                logging.warning(
                    "Found unexpected file in ingest metadata folder: %s",
                    path.file_name,
                )
                continue
            logging.info("Processing %s", path.file_name)

            col_store: Dict[str, Dict[str, IngestMetadataCounts]] = defaultdict(dict)

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
                ] = IngestMetadataCounts.from_json(struct)
            self.store[table_name][col_name] = col_store
        logging.info("DONE PROCESSING")

    def update_data_freshness_results(self) -> None:
        """Refreshes information in the metadata store about freshness of ingested data for all states."""
        bq_export_config = CloudSqlToBQConfig.for_schema_type(
            SchemaType.STATE,
            yaml_path=GcsfsFilePath.from_absolute_path(
                f"gs://{self.project_id}-configs/cloud_sql_to_bq_config.yaml"
            ),
        )
        regions_paused = (
            [] if bq_export_config is None else bq_export_config.region_codes_to_exclude
        )

        latest_upper_bounds_path = GcsfsFilePath.from_absolute_path(
            f"gs://{self.project_id}-ingest-metadata/ingest_metadata_latest_ingested_upper_bounds.json"
        )
        latest_upper_bounds_json = self.gcs_fs.download_as_string(
            latest_upper_bounds_path
        )
        latest_upper_bounds = []

        for line in latest_upper_bounds_json.splitlines():
            line = line.strip()
            if not line:
                continue
            struct = json.loads(line)
            latest_upper_bounds.append(
                {
                    "state": struct["state_code"],
                    "date": struct["processed_date"],
                    "ingestPaused": struct["state_code"] in regions_paused,
                }
            )
        self.data_freshness_results = latest_upper_bounds

    def fetch_object_counts_by_table(self) -> IngestMetadataResult:
        """
        This method calculates the total number of observed entities (both placeholder and not) per table
        by picking the counts from the column with the greatest number of entities.
        """
        results: IngestMetadataResult = defaultdict(dict)
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
                    if result.placeholder_count > max_state_placeholder[state_code]:
                        max_state_placeholder[state_code] = result.placeholder_count
            for state_code in max_state_placeholder.keys():
                results[table][state_code] = IngestMetadataCounts(
                    total_count=max_state_total[state_code],
                    placeholder_count=max_state_placeholder[state_code],
                )
        return results

    def fetch_table_nonnull_counts_by_column(self, table: str) -> IngestMetadataResult:
        """
        This code does the equivalent of:
        `SELECT state_code, col, SUM(total_count) AS total_count, SUM(placeholder_count) AS placeholder_count
        FROM column_metadata WHERE table_name=$1 AND value IS NOT NULL GROUP BY state_code, col`
        """
        if table not in self.store:
            return {}

        results: IngestMetadataResult = defaultdict(dict)
        for col, val_map in self.store[table].items():
            placeholder_count: Dict[str, int] = defaultdict(int)
            total_count: Dict[str, int] = defaultdict(int)
            for val, state_map in val_map.items():
                if val == "NULL":
                    continue
                for state_code, result in state_map.items():
                    total_count[state_code] += result.total_count
                    placeholder_count[state_code] += result.placeholder_count
            for state_code in placeholder_count.keys():
                results[col][state_code] = IngestMetadataCounts(
                    total_count=total_count[state_code],
                    placeholder_count=placeholder_count[state_code],
                )

        return results

    def fetch_column_object_counts_by_value(
        self, table: str, column: str
    ) -> IngestMetadataResult:
        """
        This code does the equivalent of:
        SELECT state_code, val, total_count, placeholder_count FROM column_metadata WHERE table_name=$1 AND col=$2
        """
        if table not in self.store or column not in self.store[table]:
            return {}
        return self.store[table][column]
