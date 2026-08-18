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
"""Tests for read_identity_cluster_overrides, run against the BQ emulator.

These tests seed the identity_cluster_override table with rows written as
literal dicts rather than through IdentityClusterOverride.to_bq_row, so they pin
the read path against the row shape BigQuery actually returns (nested repeated
records, DATE, TIMESTAMP) independently of the serializer. A parse failure in
this path fails the whole pipeline run at launch, so it must not first be
exercised by the first production override.
"""
import datetime

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Sex
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_overrides_dataset_for_tenant,
)
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    IDENTITY_CLUSTER_OVERRIDE_TABLE_ID,
    BlessedIdentityValues,
    IdentityClusterOverride,
    IdentityClusterOverrideDisposition,
    identity_cluster_override_schema_fields,
)
from recidiviz.pipelines.ingest.identity.read_identity_cluster_overrides import (
    read_identity_cluster_overrides,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_TENANT = Tenant.US_OZ
_TABLE_ADDRESS = BigQueryAddress(
    dataset_id=identity_overrides_dataset_for_tenant(_TENANT.value),
    table_id=IDENTITY_CLUSTER_OVERRIDE_TABLE_ID,
)

_BLESS_ROW = {
    "tenant": "US_OZ",
    "person_type": "JII",
    "external_ids": [
        {"external_id": "A", "id_type": "US_OZ_T1"},
        {"external_id": "B", "id_type": "US_OZ_T2"},
    ],
    "disposition": "BLESS",
    "blessed_values": {
        "given_name": "John",
        "middle_name": None,
        "surname": "Rivera",
        "name_suffix": None,
        "birthdate": "1980-01-01",
        "sex": "MALE",
        "gender": None,
        "ethnicity": None,
    },
    "conflicts_hash": "conflictshash1",
    "recorded_by": "tester",
    "recorded_at": "2026-01-02T03:04:05+00:00",
    "note": "same person; surname corrected",
}

_EXCLUDE_ROW = {
    "tenant": "US_OZ",
    "person_type": "JII",
    "external_ids": [{"external_id": "C", "id_type": "US_OZ_T1"}],
    "disposition": "EXCLUDE",
    "blessed_values": None,
    "conflicts_hash": None,
    "recorded_by": "tester",
    "recorded_at": "2026-01-02T03:04:05+00:00",
    "note": "different people",
}

_EXPECTED_BLESS = IdentityClusterOverride(
    tenant=_TENANT,
    person_type=PersonType.JII,
    external_ids=(("A", "US_OZ_T1"), ("B", "US_OZ_T2")),
    disposition=IdentityClusterOverrideDisposition.BLESS,
    blessed_values=BlessedIdentityValues(
        given_name="John",
        surname="Rivera",
        birthdate=datetime.date(1980, 1, 1),
        sex=Sex.MALE,
    ),
    conflicts_hash="conflictshash1",
    recorded_by="tester",
    recorded_at=datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
    note="same person; surname corrected",
)

_EXPECTED_EXCLUDE = IdentityClusterOverride(
    tenant=_TENANT,
    person_type=PersonType.JII,
    external_ids=(("C", "US_OZ_T1"),),
    disposition=IdentityClusterOverrideDisposition.EXCLUDE,
    blessed_values=None,
    conflicts_hash=None,
    recorded_by="tester",
    recorded_at=datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
    note="different people",
)


class ReadIdentityClusterOverridesTest(BigQueryEmulatorTestCase):
    """Tests for read_identity_cluster_overrides, run against the BQ emulator."""

    def setUp(self) -> None:
        # Created per test: the emulator harness wipes all data in tearDown, so
        # tables seeded once at class setup would vanish after the first test.
        super().setUp()
        self.create_mock_table(
            _TABLE_ADDRESS, schema=identity_cluster_override_schema_fields()
        )

    def test_reads_bless_and_exclude_rows(self) -> None:
        self.load_rows_into_table(_TABLE_ADDRESS, [_BLESS_ROW, _EXCLUDE_ROW])

        overrides = read_identity_cluster_overrides(
            project_id=self.project_id,
            dataset_id=_TABLE_ADDRESS.dataset_id,
            tenant=_TENANT,
        )

        self.assertEqual(
            _EXPECTED_BLESS,
            overrides.get_override((("A", "US_OZ_T1"), ("B", "US_OZ_T2"))),
        )
        self.assertEqual(
            _EXPECTED_EXCLUDE, overrides.get_override((("C", "US_OZ_T1"),))
        )

    def test_reads_empty_table(self) -> None:
        overrides = read_identity_cluster_overrides(
            project_id=self.project_id,
            dataset_id=_TABLE_ADDRESS.dataset_id,
            tenant=_TENANT,
        )

        self.assertIsNone(overrides.get_override((("A", "US_OZ_T1"),)))
