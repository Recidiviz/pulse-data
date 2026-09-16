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
"""Tests for the identity child row builders."""
import datetime
import uuid
from unittest import TestCase

import attr

from recidiviz.common import demographics
from recidiviz.common.constants.identity import IdentifierType, NameUse, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterAlias,
    IdentityClusterEmail,
    IdentityClusterEthnicity,
    IdentityClusterExternalId,
    IdentityClusterGender,
    IdentityClusterName,
    IdentityClusterPhoneNumber,
    IdentityClusterRace,
    IdentityClusterSex,
)
from recidiviz.services.identity.import_processing.identity_child_rows import (
    ATTRIBUTE_ROW_TYPES,
    build_attribute_rows,
)

_TENANT = Tenant.US_OZ
_ID_TYPE = IdentifierType.US_OZ_LOTR_ID
_NOW = datetime.datetime(2026, 9, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)
_RECIDIVIZ_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")

# Cluster fields the fixture legitimately leaves unpopulated.
_UNPOPULATED_FIELD_EXEMPTIONS = {
    # Validated to always be None on IdentityCluster.
    "person_type_raw_text",
}

# A cluster with every attribute field populated, so build_attribute_rows
# produces at least one row of every type it can produce. A field left
# unpopulated here would hide its row type from the lockstep test below.
_FULLY_POPULATED_CLUSTER = IdentityCluster(
    tenant=_TENANT,
    person_type=PersonType.JII,
    birthdate=datetime.date(1990, 9, 22),
    external_ids=(
        IdentityClusterExternalId(
            tenant=_TENANT, external_id="A123", id_type=_ID_TYPE.value
        ),
    ),
    name=IdentityClusterName(
        tenant=_TENANT,
        given_name="Frodo",
        preferred_name="Fro",
        surname="Baggins",
        middle_name="Drogo",
        name_suffix="Jr.",
    ),
    gender=IdentityClusterGender(tenant=_TENANT, gender=demographics.Gender.MALE),
    sex=IdentityClusterSex(tenant=_TENANT, sex=demographics.Sex.MALE),
    races=(IdentityClusterRace(tenant=_TENANT, race=demographics.Race.WHITE),),
    ethnicity=IdentityClusterEthnicity(
        tenant=_TENANT, ethnicity=demographics.Ethnicity.NOT_HISPANIC
    ),
    phone_numbers=(IdentityClusterPhoneNumber(tenant=_TENANT, number="5551234567"),),
    emails=(IdentityClusterEmail(tenant=_TENANT, address="frodo@fake.com"),),
    aliases=(
        IdentityClusterAlias(
            tenant=_TENANT,
            given_name="Mr",
            surname="Underhill",
            name_use=NameUse.ALIAS,
        ),
    ),
)


class AttributeRowTypesTest(TestCase):
    """Tests that ATTRIBUTE_ROW_TYPES stays in step with build_attribute_rows."""

    def test_attribute_row_types_matches_builder_output(self) -> None:
        """A row type build_attribute_rows produces but ATTRIBUTE_ROW_TYPES lacks
        would make the update pass add rows to a table it never clears,
        duplicating them on every import; an entry with no builder would make
        the update pass clear a table nothing repopulates."""
        rows = build_attribute_rows(
            _FULLY_POPULATED_CLUSTER,
            recidiviz_id=_RECIDIVIZ_ID,
            now=_NOW,
            committed_email_hashes=set(),
            staged_email_hashes=set(),
        )
        self.assertEqual(set(ATTRIBUTE_ROW_TYPES), {type(row) for row in rows})

    def test_fixture_populates_every_cluster_field(self) -> None:
        """The lockstep test above only sees a row type if the fixture populates
        the field that produces it, so a new IdentityCluster field added without
        updating the fixture would silently shrink the lockstep guarantee."""
        for field in attr.fields(IdentityCluster):
            if field.name in _UNPOPULATED_FIELD_EXEMPTIONS:
                continue
            self.assertTrue(
                getattr(_FULLY_POPULATED_CLUSTER, field.name),
                f"_FULLY_POPULATED_CLUSTER must populate [{field.name}] so the "
                "lockstep test sees every row type build_attribute_rows can "
                "produce; if the field legitimately stays unpopulated, add it "
                "to _UNPOPULATED_FIELD_EXEMPTIONS.",
            )
