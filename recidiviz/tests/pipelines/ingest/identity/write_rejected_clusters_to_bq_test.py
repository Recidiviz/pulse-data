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
"""Tests for WriteRejectedIdentityClustersToBQ, run against the BQ emulator."""
import datetime
from functools import partial
from unittest.mock import patch

import apache_beam as beam

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.pipelines.ingest.identity import write_rejected_clusters_to_bq
from recidiviz.pipelines.ingest.identity.rejected_identity_cluster import (
    REJECTED_IDENTITY_CLUSTER_TABLE_ID,
    RejectedIdentityCluster,
    rejected_identity_cluster_schema_fields,
)
from recidiviz.pipelines.ingest.identity.types import AttributeConflict
from recidiviz.pipelines.ingest.identity.write_rejected_clusters_to_bq import (
    WriteRejectedIdentityClustersToBQ,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.fake_bigquery import FakeWriteToBigQueryEmulator

_TENANT = Tenant.US_DD
_TABLE_ADDRESS = BigQueryAddress(
    dataset_id="us_dd_identity_rejections", table_id=REJECTED_IDENTITY_CLUSTER_TABLE_ID
)
_STALE_REJECTED_AT = datetime.datetime(2026, 7, 1, 0, 0, 0, tzinfo=datetime.UTC)
_RUN_REJECTED_AT = datetime.datetime(2026, 7, 28, 0, 0, 0, tzinfo=datetime.UTC)

_STALE_CLUSTER = RejectedIdentityCluster(
    tenant=_TENANT,
    person_type=PersonType.JII,
    external_ids=(("A", "US_DD_IDENTITY_PERSON_ID"),),
    conflicts=(AttributeConflict(field="surname", values=("GALE", "THROPP")),),
    contributing_fragment_ids=("stale_fragment_hash",),
)

_NEW_CLUSTER = RejectedIdentityCluster(
    tenant=_TENANT,
    person_type=PersonType.JII,
    external_ids=(("B", "US_DD_IDENTITY_PERSON_ID"),),
    conflicts=(
        AttributeConflict(field="birthdate", values=("1990-01-01", "1985-06-15")),
    ),
    contributing_fragment_ids=("new_fragment_hash",),
)

_CLUSTER_IDS_QUERY = (
    f"SELECT identity_cluster_id FROM `{_TABLE_ADDRESS.to_str()}` "
    f"ORDER BY identity_cluster_id"
)


class WriteRejectedIdentityClustersToBQTest(BigQueryEmulatorTestCase):
    """Tests for WriteRejectedIdentityClustersToBQ, run against the BQ emulator.
    Each run replaces the table's rows. The empty-input case is the interesting
    one: WriteToBigQuery clears the table when the run rejects nothing, so no
    prior run's rows survive."""

    def setUp(self) -> None:
        # Created per test: the emulator harness wipes all data in tearDown, so
        # tables seeded once at class setup would vanish after the first test.
        super().setUp()
        self.create_mock_table(
            _TABLE_ADDRESS, schema=rejected_identity_cluster_schema_fields()
        )

    def _run_write(self, rejected_clusters: list[RejectedIdentityCluster]) -> None:
        """Runs the transform over the given clusters against the emulator."""
        with patch(
            f"{write_rejected_clusters_to_bq.__name__}.WriteToBigQuery",
            partial(FakeWriteToBigQueryEmulator, test_case=self),
        ):
            test_pipeline = create_test_pipeline()
            _ = (
                test_pipeline
                # Create-then-flatten so an empty input list still runs.
                | beam.Create([rejected_clusters])
                | beam.FlatMap(lambda clusters: clusters)
                | WriteRejectedIdentityClustersToBQ(
                    output_dataset=_TABLE_ADDRESS.dataset_id,
                    rejected_at=_RUN_REJECTED_AT,
                )
            )
            test_pipeline.run()

    def _seed_stale_row(self) -> None:
        self.load_rows_into_table(
            _TABLE_ADDRESS,
            data=[_STALE_CLUSTER.to_bq_row(rejected_at=_STALE_REJECTED_AT)],
        )

    def test_clears_stale_rows_when_run_rejects_nothing(self) -> None:
        self._seed_stale_row()

        self._run_write([])

        self.run_query_test(_CLUSTER_IDS_QUERY, expected_result=[])

    def test_replaces_stale_rows(self) -> None:
        self._seed_stale_row()

        self._run_write([_NEW_CLUSTER])

        self.run_query_test(
            _CLUSTER_IDS_QUERY,
            expected_result=[{"identity_cluster_id": _NEW_CLUSTER.identity_cluster_id}],
        )
