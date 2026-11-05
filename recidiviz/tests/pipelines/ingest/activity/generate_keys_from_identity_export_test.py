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
"""Tests for GenerateKeysFromIdentityExport, run against the BQ emulator with
the real exported identities and external_ids schemas."""
import re
from typing import Callable, Iterable
from unittest.mock import patch

import apache_beam as beam
from apache_beam.testing.util import BeamAssertException, assert_that
from more_itertools import one

from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.entity.generate_primary_key import PrimaryKey
from recidiviz.pipelines.ingest.activity.generate_keys_from_identity_export import (
    GenerateKeysFromIdentityExport,
)
from recidiviz.pipelines.ingest.types import ExternalIdKey
from recidiviz.source_tables.identity_service_export_source_tables import (
    build_identity_service_export_source_table_collection,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.fake_bigquery import FakeReadFromBigQueryWithEmulator
from recidiviz.utils.types import assert_type

_TENANT = Tenant.US_XX

_EXPORT_COLLECTION = build_identity_service_export_source_table_collection()

_IDENTITIES_TABLE = one(
    table
    for table in _EXPORT_COLLECTION.source_tables
    if table.address.table_id == assert_type(schema.Identity.__tablename__, str)
)

_EXTERNAL_IDS_TABLE = one(
    table
    for table in _EXPORT_COLLECTION.source_tables
    if table.address.table_id == assert_type(schema.ExternalId.__tablename__, str)
)

_PERSON_RECIDIVIZ_ID = "11111111-1111-1111-1111-111111111111"
_STAFF_RECIDIVIZ_ID = "22222222-2222-2222-2222-222222222222"

_US_XX_PERSON_IDENTITY_ROW = {
    "recidiviz_id": _PERSON_RECIDIVIZ_ID,
    "created_utc": "2026-01-02T03:04:05+00:00",
    "last_updated_utc": "2026-01-02T03:04:05+00:00",
    "tenant": "US_XX",
    "person_type": "JII",
    "status": "ACTIVE",
    "merged_into": None,
    "legacy_person_id": 1234,
    "legacy_staff_id": None,
}

_US_XX_STAFF_IDENTITY_ROW = {
    "recidiviz_id": _STAFF_RECIDIVIZ_ID,
    "created_utc": "2026-01-02T03:04:05+00:00",
    "last_updated_utc": "2026-01-02T03:04:05+00:00",
    "tenant": "US_XX",
    "person_type": "STAFF",
    "status": "ACTIVE",
    "merged_into": None,
    "legacy_person_id": None,
    "legacy_staff_id": 5678,
}

_PERSON_EXTERNAL_ID_ROW = {
    "external_id": "P100",
    "id_type": "US_XX_DOC_ID",
    "recidiviz_id": _PERSON_RECIDIVIZ_ID,
    "is_active": True,
    "is_current_display_id_for_type": True,
    "is_stable_id_for_type": True,
}

_STAFF_EXTERNAL_ID_ROW = {
    "external_id": "S200",
    "id_type": "US_XX_STAFF_ID",
    "recidiviz_id": _STAFF_RECIDIVIZ_ID,
    "is_active": True,
    "is_current_display_id_for_type": True,
    "is_stable_id_for_type": True,
}

OutputPair = tuple[ExternalIdKey, PrimaryKey]


class GenerateKeysFromIdentityExportTest(BigQueryEmulatorTestCase):
    """Tests for GenerateKeysFromIdentityExport, run against the BQ emulator."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            _IDENTITIES_TABLE.address, schema=_IDENTITIES_TABLE.schema_fields
        )
        self.create_mock_table(
            _EXTERNAL_IDS_TABLE.address, schema=_EXTERNAL_IDS_TABLE.schema_fields
        )

        self.read_from_bq_patcher = patch(
            "apache_beam.io.ReadFromBigQuery",
            self.get_fake_read_from_big_query,
        )
        self.read_from_bq_patcher.start()

        self.test_pipeline = create_test_pipeline()

    def tearDown(self) -> None:
        super().tearDown()
        self.read_from_bq_patcher.stop()

    def get_fake_read_from_big_query(
        self,
        query: str,
        # pylint: disable=unused-argument
        use_standard_sql: bool,
        validate: bool,
        bigquery_job_labels: dict[str, str],
    ) -> FakeReadFromBigQueryWithEmulator:
        return FakeReadFromBigQueryWithEmulator(query=query, test_case=self)

    def _build_transform_output(self) -> beam.PCollection[OutputPair]:
        return self.test_pipeline | GenerateKeysFromIdentityExport(
            tenant=_TENANT,
            export_dataset=_IDENTITIES_TABLE.address.dataset_id,
            project_id=self.project_id,
            resource_labels={},
        )

    @staticmethod
    def get_expected_output_validator(
        expected_output: Iterable[OutputPair],
    ) -> Callable[[Iterable[OutputPair]], None]:
        def _validate_output(output: Iterable[OutputPair]) -> None:
            sorted_expected = sorted(expected_output, key=lambda pair: pair[0])
            sorted_output = sorted(output, key=lambda pair: pair[0])
            if sorted_expected != sorted_output:
                raise BeamAssertException(
                    f"Output does not match expected output. Output: {output}. "
                    f"Expected: {expected_output}."
                )

        return _validate_output

    def test_person_and_staff_identities_keyed_by_their_legacy_ids(self) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address,
            [_US_XX_PERSON_IDENTITY_ROW, _US_XX_STAFF_IDENTITY_ROW],
        )
        self.load_rows_into_table(
            _EXTERNAL_IDS_TABLE.address,
            [_PERSON_EXTERNAL_ID_ROW, _STAFF_EXTERNAL_ID_ROW],
        )

        assert_that(
            self._build_transform_output(),
            self.get_expected_output_validator(
                [
                    (("P100", "US_XX_DOC_ID"), 1234),
                    (("S200", "US_XX_STAFF_ID"), 5678),
                ]
            ),
        )
        self.test_pipeline.run()

    def test_identity_with_multiple_active_external_ids_emits_one_pair_per_id(
        self,
    ) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address, [_US_XX_PERSON_IDENTITY_ROW]
        )
        self.load_rows_into_table(
            _EXTERNAL_IDS_TABLE.address,
            [
                _PERSON_EXTERNAL_ID_ROW,
                {
                    "external_id": "K900",
                    "id_type": "US_XX_CONTROL_ID",
                    "recidiviz_id": _PERSON_RECIDIVIZ_ID,
                    "is_active": True,
                    "is_current_display_id_for_type": True,
                    "is_stable_id_for_type": True,
                },
            ],
        )

        assert_that(
            self._build_transform_output(),
            self.get_expected_output_validator(
                [
                    (("P100", "US_XX_DOC_ID"), 1234),
                    (("K900", "US_XX_CONTROL_ID"), 1234),
                ]
            ),
        )
        self.test_pipeline.run()

    def test_inactive_external_ids_are_excluded(self) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address, [_US_XX_PERSON_IDENTITY_ROW]
        )
        self.load_rows_into_table(
            _EXTERNAL_IDS_TABLE.address,
            [
                _PERSON_EXTERNAL_ID_ROW,
                # Identical to the active row's shape except is_active: residue
                # of a split, kept for audit history only.
                {
                    "external_id": "P101",
                    "id_type": "US_XX_DOC_ID",
                    "recidiviz_id": _PERSON_RECIDIVIZ_ID,
                    "is_active": False,
                    "is_current_display_id_for_type": False,
                    "is_stable_id_for_type": False,
                },
            ],
        )

        assert_that(
            self._build_transform_output(),
            self.get_expected_output_validator([(("P100", "US_XX_DOC_ID"), 1234)]),
        )
        self.test_pipeline.run()

    def test_retired_identity_external_ids_are_excluded(self) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address,
            [
                _US_XX_PERSON_IDENTITY_ROW,
                # Identical to the active person identity except its lifecycle
                # columns: RETIRED after a merge into the surviving identity.
                {
                    "recidiviz_id": "33333333-3333-3333-3333-333333333333",
                    "created_utc": "2026-01-02T03:04:05+00:00",
                    "last_updated_utc": "2026-01-02T03:04:05+00:00",
                    "tenant": "US_XX",
                    "person_type": "JII",
                    "status": "RETIRED",
                    "merged_into": _PERSON_RECIDIVIZ_ID,
                    "legacy_person_id": 4321,
                    "legacy_staff_id": None,
                },
            ],
        )
        self.load_rows_into_table(
            _EXTERNAL_IDS_TABLE.address,
            [
                _PERSON_EXTERNAL_ID_ROW,
                {
                    "external_id": "P300",
                    "id_type": "US_XX_DOC_ID",
                    "recidiviz_id": "33333333-3333-3333-3333-333333333333",
                    "is_active": True,
                    "is_current_display_id_for_type": True,
                    "is_stable_id_for_type": True,
                },
            ],
        )

        assert_that(
            self._build_transform_output(),
            self.get_expected_output_validator([(("P100", "US_XX_DOC_ID"), 1234)]),
        )
        self.test_pipeline.run()

    def test_recidiviz_employee_identities_are_excluded(self) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address,
            [
                _US_XX_PERSON_IDENTITY_ROW,
                # RECIDIVIZ_EMPLOYEE identities deliberately have both legacy
                # id columns NULL; the person_type filter must exclude them
                # before the keyless-row check fires.
                {
                    "recidiviz_id": "44444444-4444-4444-4444-444444444444",
                    "created_utc": "2026-01-02T03:04:05+00:00",
                    "last_updated_utc": "2026-01-02T03:04:05+00:00",
                    "tenant": "US_XX",
                    "person_type": "RECIDIVIZ_EMPLOYEE",
                    "status": "ACTIVE",
                    "merged_into": None,
                    "legacy_person_id": None,
                    "legacy_staff_id": None,
                },
            ],
        )
        self.load_rows_into_table(
            _EXTERNAL_IDS_TABLE.address,
            [
                _PERSON_EXTERNAL_ID_ROW,
                {
                    "external_id": "E400",
                    "id_type": "US_XX_BADGE_ID",
                    "recidiviz_id": "44444444-4444-4444-4444-444444444444",
                    "is_active": True,
                    "is_current_display_id_for_type": True,
                    "is_stable_id_for_type": True,
                },
            ],
        )

        assert_that(
            self._build_transform_output(),
            self.get_expected_output_validator([(("P100", "US_XX_DOC_ID"), 1234)]),
        )
        self.test_pipeline.run()

    def test_other_tenant_identities_are_excluded(self) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address,
            [
                _US_XX_PERSON_IDENTITY_ROW,
                # Identical to the US_XX person identity except tenant: the
                # export dataset is shared across tenants.
                {
                    "recidiviz_id": "55555555-5555-5555-5555-555555555555",
                    "created_utc": "2026-01-02T03:04:05+00:00",
                    "last_updated_utc": "2026-01-02T03:04:05+00:00",
                    "tenant": "US_YY",
                    "person_type": "JII",
                    "status": "ACTIVE",
                    "merged_into": None,
                    "legacy_person_id": 9999,
                    "legacy_staff_id": None,
                },
            ],
        )
        self.load_rows_into_table(
            _EXTERNAL_IDS_TABLE.address,
            [
                _PERSON_EXTERNAL_ID_ROW,
                {
                    "external_id": "ND500",
                    "id_type": "US_YY_SID",
                    "recidiviz_id": "55555555-5555-5555-5555-555555555555",
                    "is_active": True,
                    "is_current_display_id_for_type": True,
                    "is_stable_id_for_type": True,
                },
            ],
        )

        assert_that(
            self._build_transform_output(),
            self.get_expected_output_validator([(("P100", "US_XX_DOC_ID"), 1234)]),
        )
        self.test_pipeline.run()

    def test_raises_when_keyed_identity_has_active_external_ids_but_no_legacy_id(
        self,
    ) -> None:
        self.load_rows_into_table(
            _IDENTITIES_TABLE.address,
            [
                # Identical to the active person identity except its legacy id
                # columns: the export computed no key for an identity with
                # active external IDs.
                {
                    "recidiviz_id": _PERSON_RECIDIVIZ_ID,
                    "created_utc": "2026-01-02T03:04:05+00:00",
                    "last_updated_utc": "2026-01-02T03:04:05+00:00",
                    "tenant": "US_XX",
                    "person_type": "JII",
                    "status": "ACTIVE",
                    "merged_into": None,
                    "legacy_person_id": None,
                    "legacy_staff_id": None,
                }
            ],
        )
        self.load_rows_into_table(
            _EXTERNAL_IDS_TABLE.address, [_PERSON_EXTERNAL_ID_ROW]
        )

        _ = self._build_transform_output()
        # Beam wraps in-pipeline exceptions, so match the message fragment
        # rather than the full anchored error.
        with self.assertRaisesRegex(
            Exception,
            re.escape(
                f"Identity [{_PERSON_RECIDIVIZ_ID}] is an active JII identity "
                f"with active external IDs (including ('P100', 'US_XX_DOC_ID')) "
                f"but a NULL legacy_person_id"
            ),
        ):
            self.test_pipeline.run()
