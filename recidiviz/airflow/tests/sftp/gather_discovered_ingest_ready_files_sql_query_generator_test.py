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
"""Unit tests for GatherDiscoveredIngestReadyFilesSqlQueryGenerator"""
import unittest

from recidiviz.airflow.dags.sftp.gather_discovered_ingest_ready_files_sql_query_generator import (
    GatherDiscoveredIngestReadyFilesSqlQueryGenerator,
)


class TestGatherDiscoveredIngestReadyFilesSqlQueryGenerator(unittest.TestCase):
    """Unit tests for GatherDiscoveredIngestReadyFilesSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = GatherDiscoveredIngestReadyFilesSqlQueryGenerator(
            region_code="US_XX"
        )

    def test_generates_sql_correctly(self) -> None:
        expected_query = """
SELECT post_processed_normalized_file_path, remote_file_path FROM direct_ingest_sftp_ingest_ready_file_metadata
 WHERE region_code = 'US_XX' AND file_upload_time IS NULL AND
 file_discovery_time IS NOT NULL
 ORDER BY file_discovery_time, post_processed_normalized_file_path
 LIMIT 1000;"""

        self.assertEqual(expected_query, self.generator.sql_query)
