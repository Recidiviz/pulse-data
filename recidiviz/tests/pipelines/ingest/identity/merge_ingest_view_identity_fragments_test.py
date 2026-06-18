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
"""Tests for merging IdentityFragments via MergeIngestViewRootEntityTrees."""
import datetime
import unittest

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Gender, Race
from recidiviz.persistence.entity.identity import (
    identity_fragment_entities as identity_entities,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityExternalId,
    IdentityFragment,
    IdentityGender,
    IdentityName,
    IdentityPhoneNumber,
    IdentityRace,
)
from recidiviz.pipelines.ingest.transforms.merge_root_entity_trees import (
    MergeIngestViewRootEntityTrees,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TENANT = Tenant.US_XX


def _make_attrs(
    *,
    name: IdentityName | None = None,
    birthdate: datetime.date | None = None,
    gender: IdentityGender | None = None,
    races: list[IdentityRace] | None = None,
    phone_numbers: list[IdentityPhoneNumber] | None = None,
) -> IdentityAttributes:
    return IdentityAttributes(
        tenant=_TENANT,
        person_type=PersonType.JII,
        name=name,
        birthdate=birthdate,
        gender=gender,
        races=races or [],
        phone_numbers=phone_numbers or [],
    )


def _make_fragment(
    external_ids: list[tuple[str, str]],
    attrs: IdentityAttributes,
) -> IdentityFragment:
    return IdentityFragment(
        tenant=_TENANT,
        external_ids=[
            IdentityExternalId(tenant=_TENANT, external_id=eid, id_type=etype)
            for eid, etype in external_ids
        ],
        attributes=attrs,
    )


_UPPER_BOUND_TS = datetime.datetime(2024, 1, 15).timestamp()
_VIEW_NAME = "eg_person"


class TestMergeIngestViewIdentityFragmentsPTransform(unittest.TestCase):
    """Tests merging IdentityFragments using the shared MergeIngestViewRootEntityTrees
    PTransform with the identity entities module."""

    def test_two_fragments_same_key_merged(self) -> None:
        """Two fragments sharing the same external ID and date are merged.

        frag1 has P1, frag2 has P1 and B1. The P1 group merges both fragments (union of
        external IDs, merged attributes). The B1 group only has frag2 so it passes through
        unmerged.

        The merge exercises flat fields (birthdate), singular forward edges (name,
        gender), and list forward edges (races, phone_numbers).
        """
        name = IdentityName(tenant=_TENANT, given_name="JOHN", surname="DOE")
        gender = IdentityGender(tenant=_TENANT, gender=Gender.MALE)
        race = IdentityRace(tenant=_TENANT, race=Race.WHITE)
        phone = IdentityPhoneNumber(tenant=_TENANT, number="5550100001")

        frag1 = _make_fragment(
            [("P1", "US_XX_EG")],
            _make_attrs(
                name=name,
                birthdate=datetime.date(1990, 1, 1),
                races=[race],
            ),
        )
        frag2 = _make_fragment(
            [("P1", "US_XX_EG"), ("B1", "US_XX_BOOKING")],
            _make_attrs(
                birthdate=datetime.date(1990, 1, 1),
                gender=gender,
                phone_numbers=[phone],
            ),
        )

        # P1 group: frag1 + frag2 → merged (both ext IDs, merged attrs)
        expected_p1_merged = IdentityFragment(
            tenant=_TENANT,
            external_ids=[
                IdentityExternalId(
                    tenant=_TENANT, external_id="P1", id_type="US_XX_EG"
                ),
                IdentityExternalId(
                    tenant=_TENANT, external_id="B1", id_type="US_XX_BOOKING"
                ),
            ],
            attributes=_make_attrs(
                name=name,
                birthdate=datetime.date(1990, 1, 1),
                gender=gender,
                races=[race],
                phone_numbers=[phone],
            ),
        )

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        (_UPPER_BOUND_TS, frag1),
                        (_UPPER_BOUND_TS, frag2),
                    ]
                )
                | MergeIngestViewRootEntityTrees(
                    _VIEW_NAME, entities_module=identity_entities
                )
            )
            assert_that(
                result,
                equal_to(
                    [
                        (
                            ("P1", "US_XX_EG"),
                            (_UPPER_BOUND_TS, _VIEW_NAME, expected_p1_merged),
                        ),
                        (
                            ("B1", "US_XX_BOOKING"),
                            (_UPPER_BOUND_TS, _VIEW_NAME, frag2),
                        ),
                    ]
                ),
            )
