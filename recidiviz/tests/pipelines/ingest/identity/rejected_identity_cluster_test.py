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
"""Tests for RejectedIdentityCluster."""
import datetime
import unittest

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Sex
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
)
from recidiviz.pipelines.ingest.identity.rejected_identity_cluster import (
    RejectedIdentityCluster,
    rejected_identity_cluster_schema_fields,
)
from recidiviz.pipelines.ingest.identity.types import AttributeConflict

_REJECTED_AT = datetime.datetime(2026, 7, 28, 12, 0, 0, tzinfo=datetime.UTC)

# The conflict values are the rich types the checker produces (strings, dates,
# and enums); to_bq_row serializes them into the string column.
_REJECTED_CLUSTER = RejectedIdentityCluster(
    tenant=Tenant.US_OZ,
    person_type=PersonType.JII,
    external_ids=(("A", "US_OZ_T1"), ("B", "US_OZ_T2")),
    conflicts=(
        AttributeConflict(field="surname", values=("WILLIAMS", "JOHNSON")),
        AttributeConflict(
            field="birthdate",
            values=(datetime.date(1990, 1, 1), datetime.date(1985, 6, 15)),
        ),
        AttributeConflict(field="sex", values=(Sex.MALE, Sex.FEMALE)),
    ),
    contributing_fragment_ids=("fragment_hash_1", "fragment_hash_2"),
)


class TestRejectedIdentityCluster(unittest.TestCase):
    """Tests for RejectedIdentityCluster."""

    def test_to_bq_row(self) -> None:
        self.assertEqual(
            _REJECTED_CLUSTER.to_bq_row(rejected_at=_REJECTED_AT),
            {
                "rejected_at": "2026-07-28T12:00:00+00:00",
                "tenant": "US_OZ",
                "person_type": "JII",
                "identity_cluster_id": _REJECTED_CLUSTER.identity_cluster_id,
                "external_ids": [
                    {"external_id": "A", "id_type": "US_OZ_T1"},
                    {"external_id": "B", "id_type": "US_OZ_T2"},
                ],
                "recorded_conflicts": [
                    {"field": "surname", "values": ["WILLIAMS", "JOHNSON"]},
                    {"field": "birthdate", "values": ["1990-01-01", "1985-06-15"]},
                    {"field": "sex", "values": ["MALE", "FEMALE"]},
                ],
                "contributing_fragment_ids": ["fragment_hash_1", "fragment_hash_2"],
            },
        )

    def test_identity_cluster_id_derived_from_external_ids_only(self) -> None:
        """The rejected cluster's id matches the id a kept cluster with the same
        external ids carries, regardless of the kept cluster's attributes. This
        guards the assumption that identity_cluster_id hashes external ids only."""
        kept_cluster_with_attributes = IdentityCluster(
            tenant=Tenant.US_OZ,
            person_type=PersonType.JII,
            external_ids=(
                IdentityClusterExternalId(
                    tenant=Tenant.US_OZ, external_id="A", id_type="US_OZ_T1"
                ),
                IdentityClusterExternalId(
                    tenant=Tenant.US_OZ, external_id="B", id_type="US_OZ_T2"
                ),
            ),
            birthdate=datetime.date(1990, 1, 1),
        )
        self.assertEqual(
            _REJECTED_CLUSTER.identity_cluster_id,
            kept_cluster_with_attributes.identity_cluster_id,
        )

    def test_to_bq_row_naive_timestamp_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Expected a timezone-aware rejected_at"
        ):
            _REJECTED_CLUSTER.to_bq_row(
                rejected_at=datetime.datetime(2026, 7, 28, 12, 0, 0)
            )

    def test_row_matches_schema(self) -> None:
        """Guards that to_bq_row and the table schema stay in sync: every
        schema column appears in the row (including the nested record fields)
        and the row carries no column the schema does not declare."""
        row = _REJECTED_CLUSTER.to_bq_row(rejected_at=_REJECTED_AT)
        schema_by_name = {
            field.name: field for field in rejected_identity_cluster_schema_fields()
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
