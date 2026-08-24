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
"""A BigQueryEmulatorTestCase whose client's GCS-interacting methods are backed by
an in-memory FakeGCSFileSystem (exposed as self.fs).
"""

import csv
import io
from typing import Any
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class _FakeGcsCsvLoader:
    """Stand-in for BigQueryClientImpl.load_table_from_cloud_storage that reads the CSVs
    at the given source URIs out of a FakeGCSFileSystem and streams them into the
    destination emulator table. The emulator cannot load from a gs:// URI backed by the
    fake filesystem, so this replaces only that GCS->BQ transport step; every other query
    in the run executes for real against the emulator."""

    def __init__(self, *, fs: FakeGCSFileSystem, bq_client: BigQueryClient) -> None:
        self.fs = fs
        self.bq_client = bq_client

    def __call__(
        self,
        *,
        source_uris: list[str],
        destination_address: BigQueryAddress,
        destination_table_schema: list[bigquery.SchemaField],
        **_kwargs: Any,
    ) -> mock.MagicMock:
        column_names = [field.name for field in destination_table_schema]
        rows = [
            dict(zip(column_names, values))
            for uri in source_uris
            for values in self._read_csv_rows(uri)
        ]
        if rows:
            self.bq_client.stream_into_table(destination_address, rows=rows)
        return mock.MagicMock()

    def _read_csv_rows(self, source_uri: str) -> list[list[str | None]]:
        # source_uri is a "<directory>/*.csv" glob; read every CSV under that directory
        # out of the fake filesystem.
        directory = GcsfsDirectoryPath.from_absolute_path(source_uri.rsplit("/", 1)[0])
        rows: list[list[str | None]] = []
        for path in self.fs.ls(
            directory.bucket_name, blob_prefix=directory.relative_path
        ):
            if not isinstance(path, GcsfsFilePath) or not path.abs_path().endswith(
                ".csv"
            ):
                continue
            contents = self.fs.download_as_string(path)
            rows.extend(
                [value or None for value in row]
                for row in csv.reader(io.StringIO(contents))
            )
        return rows


class BigQueryEmulatorWithGCSTestCase(BigQueryEmulatorTestCase):
    """A BigQueryEmulatorTestCase whose client's GCS-interacting methods are backed by
    an in-memory FakeGCSFileSystem exposed as self.fs.

    The BQ emulator has no GCS client, so any BigQueryClient method that moves data
    between GCS and BigQuery cannot execute against it. This subclass stands in for
    those transport steps with the fake filesystem; every other query still executes
    for real against the emulator."""

    fs: FakeGCSFileSystem

    def setUp(self) -> None:
        super().setUp()
        self.fs = FakeGCSFileSystem()
        self.enterContext(
            mock.patch.object(
                self.bq_client,
                "load_table_from_cloud_storage",
                _FakeGcsCsvLoader(fs=self.fs, bq_client=self.bq_client),
            )
        )
