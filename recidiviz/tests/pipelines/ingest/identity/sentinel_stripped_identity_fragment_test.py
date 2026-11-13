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
"""Tests for SentinelStrippedIdentityFragment."""
import datetime
import unittest

from recidiviz.common.constants.tenants import Tenant
from recidiviz.pipelines.ingest.identity.sentinel_stripped_identity_fragment import (
    SentinelStrippedIdentityFragment,
    sentinel_stripped_identity_fragment_schema_fields,
)

_STRIPPED_AT = datetime.datetime(2026, 10, 28, 12, 0, 0, tzinfo=datetime.UTC)
_UPPER_BOUND_TS = datetime.datetime(
    2026, 10, 1, 0, 0, 0, tzinfo=datetime.UTC
).timestamp()

_PARTIALLY_STRIPPED_FRAGMENT = SentinelStrippedIdentityFragment(
    tenant=Tenant.US_OZ,
    ingest_view_name="oz_person",
    upper_bound_date=_UPPER_BOUND_TS,
    prestripped_identity_fragment_id="prestrip_hash_1",
    stripped_external_ids=(("000000", "US_OZ_T1"),),
    surviving_external_ids=(("A", "US_OZ_T2"),),
)

_FULLY_DROPPED_FRAGMENT = SentinelStrippedIdentityFragment(
    tenant=Tenant.US_OZ,
    ingest_view_name="oz_person",
    upper_bound_date=_UPPER_BOUND_TS,
    prestripped_identity_fragment_id="prestrip_hash_2",
    stripped_external_ids=(("000000", "US_OZ_T1"), ("999999", "US_OZ_T2")),
    surviving_external_ids=(),
)


class TestSentinelStrippedIdentityFragment(unittest.TestCase):
    """Tests for SentinelStrippedIdentityFragment."""

    def test_to_bq_row(self) -> None:
        self.assertEqual(
            _PARTIALLY_STRIPPED_FRAGMENT.to_bq_row(stripped_at=_STRIPPED_AT),
            {
                "stripped_at": "2026-10-28T12:00:00+00:00",
                "tenant": "US_OZ",
                "ingest_view_name": "oz_person",
                "upper_bound_date": "2026-10-01T00:00:00+00:00",
                "prestripped_identity_fragment_id": "prestrip_hash_1",
                "stripped_external_ids": [
                    {"external_id": "000000", "id_type": "US_OZ_T1"},
                ],
                "surviving_external_ids": [
                    {"external_id": "A", "id_type": "US_OZ_T2"},
                ],
                "fully_dropped": False,
            },
        )

    def test_to_bq_row_fully_dropped(self) -> None:
        self.assertEqual(
            _FULLY_DROPPED_FRAGMENT.to_bq_row(stripped_at=_STRIPPED_AT),
            {
                "stripped_at": "2026-10-28T12:00:00+00:00",
                "tenant": "US_OZ",
                "ingest_view_name": "oz_person",
                "upper_bound_date": "2026-10-01T00:00:00+00:00",
                "prestripped_identity_fragment_id": "prestrip_hash_2",
                "stripped_external_ids": [
                    {"external_id": "000000", "id_type": "US_OZ_T1"},
                    {"external_id": "999999", "id_type": "US_OZ_T2"},
                ],
                "surviving_external_ids": [],
                "fully_dropped": True,
            },
        )

    def test_to_bq_row_naive_timestamp_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expected a timezone-aware stripped_at, "
            r"found \[2026-10-28 12:00:00\]\.$",
        ):
            _PARTIALLY_STRIPPED_FRAGMENT.to_bq_row(
                stripped_at=datetime.datetime(2026, 10, 28, 12, 0, 0)
            )

    def test_stripped_external_ids_must_be_non_empty(self) -> None:
        """A record exists only because something was stripped, so an empty
        stripped_external_ids is a construction bug."""
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[stripped_external_ids\] on \[SentinelStrippedIdentityFragment\] "
            r"must be a non-empty tuple\. Found value \[\(\)\]$",
        ):
            SentinelStrippedIdentityFragment(
                tenant=Tenant.US_OZ,
                ingest_view_name="oz_person",
                upper_bound_date=_UPPER_BOUND_TS,
                prestripped_identity_fragment_id="prestrip_hash_1",
                stripped_external_ids=(),
                surviving_external_ids=(("A", "US_OZ_T2"),),
            )

    def test_row_matches_schema(self) -> None:
        """Guards that to_bq_row and the table schema stay in sync: every
        schema column appears in the row (including the nested record fields)
        and the row carries no column the schema does not declare."""
        row = _PARTIALLY_STRIPPED_FRAGMENT.to_bq_row(stripped_at=_STRIPPED_AT)
        schema_by_name = {
            field.name: field
            for field in sentinel_stripped_identity_fragment_schema_fields()
        }

        self.assertEqual(set(row.keys()), set(schema_by_name.keys()))

        for column, field in schema_by_name.items():
            if field.field_type != "RECORD":
                continue
            sub_field_names = {sub_field.name for sub_field in field.fields}
            values = row[column]
            assert isinstance(values, list)
            for entry in values:
                self.assertEqual(set(entry.keys()), sub_field_names)
