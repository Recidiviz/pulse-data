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
"""Tests for check_identity_service_export_has_usable_rows, run against the BQ
emulator with the real exported identities schema."""
import re

from more_itertools import one

from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.pipelines.ingest.activity.check_identity_service_export_has_usable_rows import (
    check_identity_service_export_has_usable_rows,
)
from recidiviz.source_tables.identity_service_export_source_tables import (
    build_identity_service_export_source_table_collection,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.utils.types import assert_type

_TENANT = Tenant.US_XX

_IDENTITIES_TABLE = one(
    table
    for table in build_identity_service_export_source_table_collection().source_tables
    if table.address.table_id == assert_type(schema.Identity.__tablename__, str)
)

_US_XX_PERSON_ROW = {
    "recidiviz_id": "11111111-1111-1111-1111-111111111111",
    "created_utc": "2026-01-02T03:04:05+00:00",
    "last_updated_utc": "2026-01-02T03:04:05+00:00",
    "tenant": "US_XX",
    "person_type": "JII",
    "status": "ACTIVE",
    "merged_into": None,
    "legacy_person_id": 1234,
    "legacy_staff_id": None,
}

_US_XX_STAFF_ROW = {
    "recidiviz_id": "22222222-2222-2222-2222-222222222222",
    "created_utc": "2026-01-02T03:04:05+00:00",
    "last_updated_utc": "2026-01-02T03:04:05+00:00",
    "tenant": "US_XX",
    "person_type": "STAFF",
    "status": "ACTIVE",
    "merged_into": None,
    "legacy_person_id": None,
    "legacy_staff_id": 5678,
}

# An identity with no legacy ids at all, e.g. one with no active external IDs.
_US_XX_NULL_LEGACY_IDS_ROW = {
    "recidiviz_id": "33333333-3333-3333-3333-333333333333",
    "created_utc": "2026-01-02T03:04:05+00:00",
    "last_updated_utc": "2026-01-02T03:04:05+00:00",
    "tenant": "US_XX",
    "person_type": "JII",
    "status": "ACTIVE",
    "merged_into": None,
    "legacy_person_id": None,
    "legacy_staff_id": None,
}

# A usable row for a different tenant, which must not satisfy a US_XX check:
# the export dataset is shared across tenants.
_US_YY_PERSON_ROW = {
    "recidiviz_id": "44444444-4444-4444-4444-444444444444",
    "created_utc": "2026-01-02T03:04:05+00:00",
    "last_updated_utc": "2026-01-02T03:04:05+00:00",
    "tenant": "US_YY",
    "person_type": "JII",
    "status": "ACTIVE",
    "merged_into": None,
    "legacy_person_id": 1234,
    "legacy_staff_id": None,
}


class CheckIdentityServiceExportHasUsableRowsTest(BigQueryEmulatorTestCase):
    """Tests for check_identity_service_export_has_usable_rows, run against the
    BQ emulator."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            _IDENTITIES_TABLE.address, schema=_IDENTITIES_TABLE.schema_fields
        )

    def _expected_error_regex(self) -> str:
        return (
            "^"
            + re.escape(
                f"The activity pipeline for tenant [US_XX] is set to use the "
                f"Identity Service export, but "
                f"[{self.project_id}.identity_service_export.identities] has "
                f"no rows for that tenant with a populated legacy_person_id "
                f"or legacy_staff_id. Populate the export first by running "
                f"`python -m recidiviz.tools.identity.run_identity_service_export "
                f"--project-id {self.project_id} --tenant US_XX`, or for a "
                f"sandbox run, point the pipeline at a populated copy with "
                f'`--input_dataset_overrides_json \'{{"identity_service_export": '
                f'"<populated copy>"}}\'`.'
            )
            + "$"
        )

    def test_passes_when_tenant_has_row_with_legacy_person_id(self) -> None:
        self.load_rows_into_table(_IDENTITIES_TABLE.address, [_US_XX_PERSON_ROW])

        check_identity_service_export_has_usable_rows(
            project_id=self.project_id,
            dataset_id=_IDENTITIES_TABLE.address.dataset_id,
            tenant=_TENANT,
        )

    def test_passes_when_tenant_has_row_with_legacy_staff_id(self) -> None:
        self.load_rows_into_table(_IDENTITIES_TABLE.address, [_US_XX_STAFF_ROW])

        check_identity_service_export_has_usable_rows(
            project_id=self.project_id,
            dataset_id=_IDENTITIES_TABLE.address.dataset_id,
            tenant=_TENANT,
        )

    def test_raises_on_empty_table(self) -> None:
        with self.assertRaisesRegex(ValueError, self._expected_error_regex()):
            check_identity_service_export_has_usable_rows(
                project_id=self.project_id,
                dataset_id=_IDENTITIES_TABLE.address.dataset_id,
                tenant=_TENANT,
            )

    def test_raises_when_only_other_tenants_have_usable_rows(self) -> None:
        self.load_rows_into_table(_IDENTITIES_TABLE.address, [_US_YY_PERSON_ROW])

        with self.assertRaisesRegex(ValueError, self._expected_error_regex()):
            check_identity_service_export_has_usable_rows(
                project_id=self.project_id,
                dataset_id=_IDENTITIES_TABLE.address.dataset_id,
                tenant=_TENANT,
            )

    def test_raises_when_tenant_rows_all_have_null_legacy_ids(self) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address, [_US_XX_NULL_LEGACY_IDS_ROW]
        )

        with self.assertRaisesRegex(ValueError, self._expected_error_regex()):
            check_identity_service_export_has_usable_rows(
                project_id=self.project_id,
                dataset_id=_IDENTITIES_TABLE.address.dataset_id,
                tenant=_TENANT,
            )
