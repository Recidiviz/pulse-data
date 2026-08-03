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
"""Tests for resolve_cluster_attributes."""
import datetime
import unittest

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityEmail,
    IdentityEthnicity,
    IdentityExternalId,
    IdentityFragment,
    IdentityGender,
    IdentityName,
    IdentityPhoneNumber,
    IdentityRace,
    IdentitySex,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
    IdentityIngestPipelineTenantConfig,
    ResolutionStrategy,
)
from recidiviz.pipelines.ingest.identity.resolve_cluster_attributes import (
    resolve_cluster_attributes,
)

_TENANT = Tenant.US_OZ
_DEFAULT_CONFIG = IdentityIngestPipelineTenantConfig().resolution_strategy_config
_GENDER_LATEST_CONFIG = {
    **_DEFAULT_CONFIG,
    ConflictCheckedAttribute.GENDER: ResolutionStrategy.KEEP_LATEST,
}
_SURNAME_NULL_CONFIG = {
    **_DEFAULT_CONFIG,
    ConflictCheckedAttribute.SURNAME: ResolutionStrategy.SET_NULL,
}
_BIRTHDATE_NULL_CONFIG = {
    **_DEFAULT_CONFIG,
    ConflictCheckedAttribute.BIRTHDATE: ResolutionStrategy.SET_NULL,
}
_NAME_SUFFIX_NULL_CONFIG = {
    **_DEFAULT_CONFIG,
    ConflictCheckedAttribute.NAME_SUFFIX: ResolutionStrategy.SET_NULL,
}


def _fragment(
    *,
    external_id: str = "A",
    given_name: str | None = None,
    surname: str | None = None,
    name_suffix: str | None = None,
    birthdate: datetime.date | None = None,
    gender: Gender | None = None,
    races: list[Race] | None = None,
) -> IdentityFragment:
    """Builds a JII fragment carrying the given attributes; an all-None call
    yields an external-id-only fragment with no attributes."""
    name = (
        IdentityName(
            tenant=_TENANT,
            given_name=given_name,
            surname=surname,
            name_suffix=name_suffix,
        )
        if given_name or surname or name_suffix
        else None
    )
    attributes = (
        IdentityAttributes(
            tenant=_TENANT,
            name=name,
            birthdate=birthdate,
            gender=IdentityGender(tenant=_TENANT, gender=gender) if gender else None,
            races=[IdentityRace(tenant=_TENANT, race=race) for race in (races or [])],
        )
        if (name or birthdate or gender or races)
        else None
    )
    return IdentityFragment(
        tenant=_TENANT,
        external_ids=[
            IdentityExternalId(
                tenant=_TENANT, external_id=external_id, id_type="US_OZ_T1"
            )
        ],
        person_type=PersonType.JII,
        attributes=attributes,
    )


class TestResolveClusterAttributes(unittest.TestCase):
    """Tests for resolve_cluster_attributes."""

    def test_no_attributes_returns_none(self) -> None:
        self.assertIsNone(
            resolve_cluster_attributes(
                tenant=_TENANT,
                sorted_fragments=[
                    _fragment(external_id="A"),
                    _fragment(external_id="B"),
                ],
                resolution_strategy_config=_DEFAULT_CONFIG,
            )
        )

    def test_name_components_take_latest_across_fragments(self) -> None:
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(given_name="JOHN", surname="SMITH"),
                _fragment(given_name="JON"),
            ],
            resolution_strategy_config=_DEFAULT_CONFIG,
        )
        assert resolved is not None and resolved.name is not None
        # given_name from the later fragment; surname from the earlier (latest
        # non-null).
        self.assertEqual(resolved.name.given_name, "JON")
        self.assertEqual(resolved.name.surname, "SMITH")

    def test_birthdate_takes_latest(self) -> None:
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(birthdate=datetime.date(1990, 1, 1)),
                _fragment(birthdate=datetime.date(1990, 1, 2)),
            ],
            resolution_strategy_config=_DEFAULT_CONFIG,
        )
        assert resolved is not None
        self.assertEqual(resolved.birthdate, datetime.date(1990, 1, 2))

    def test_surname_divergence_nulls_when_configured(self) -> None:
        # A benign surname drift nulls out when the tenant configures surname
        # resolution to null; other name components keep their own default.
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(surname="SMITH", given_name="JOHN"),
                _fragment(surname="SMYTH", given_name="JOHN"),
            ],
            resolution_strategy_config=_SURNAME_NULL_CONFIG,
        )
        assert resolved is not None and resolved.name is not None
        self.assertIsNone(resolved.name.surname)
        self.assertEqual(resolved.name.given_name, "JOHN")

    def test_normalized_equal_surnames_are_not_divergent(self) -> None:
        # Punctuation variants record the same surname after normalization, so
        # a null resolution strategy keeps the latest value instead of nulling.
        # The stored value is the raw form from the latest fragment; the
        # normalized form is used only to decide divergence.
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(surname="LOPEZ SMITH", given_name="JOHN"),
                _fragment(surname="Lopez-Smith", given_name="JOHN"),
            ],
            resolution_strategy_config=_SURNAME_NULL_CONFIG,
        )
        assert resolved is not None and resolved.name is not None
        self.assertEqual(resolved.name.surname, "Lopez-Smith")

    def test_equivalent_name_suffixes_are_not_divergent(self) -> None:
        # JR and Junior canonicalize to the same suffix, so a null resolution
        # strategy keeps the latest value instead of nulling. The stored value
        # is the raw JR, not the canonical II.
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(surname="SMITH", name_suffix="Junior"),
                _fragment(surname="SMITH", name_suffix="JR"),
            ],
            resolution_strategy_config=_NAME_SUFFIX_NULL_CONFIG,
        )
        assert resolved is not None and resolved.name is not None
        self.assertEqual(resolved.name.name_suffix, "JR")

    def test_birthdate_divergence_nulls_when_configured(self) -> None:
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(surname="SMITH", birthdate=datetime.date(1990, 1, 1)),
                _fragment(surname="SMITH", birthdate=datetime.date(1990, 1, 2)),
            ],
            resolution_strategy_config=_BIRTHDATE_NULL_CONFIG,
        )
        assert resolved is not None
        self.assertIsNone(resolved.birthdate)

    def test_gender_divergence_nulls_by_default(self) -> None:
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(gender=Gender.MALE, surname="SMITH"),
                _fragment(gender=Gender.FEMALE, surname="SMITH"),
            ],
            resolution_strategy_config=_DEFAULT_CONFIG,
        )
        assert resolved is not None
        self.assertIsNone(resolved.gender)

    def test_gender_divergence_takes_latest_when_configured(self) -> None:
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(gender=Gender.MALE, surname="SMITH"),
                _fragment(gender=Gender.FEMALE, surname="SMITH"),
            ],
            resolution_strategy_config=_GENDER_LATEST_CONFIG,
        )
        assert resolved is not None and resolved.gender is not None
        self.assertEqual(resolved.gender.gender, Gender.FEMALE)

    def test_gender_single_value_kept(self) -> None:
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(gender=Gender.MALE, surname="SMITH"),
                _fragment(surname="SMITH"),
            ],
            resolution_strategy_config=_DEFAULT_CONFIG,
        )
        assert resolved is not None and resolved.gender is not None
        self.assertEqual(resolved.gender.gender, Gender.MALE)

    def test_races_are_unioned(self) -> None:
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(races=[Race.WHITE]),
                _fragment(races=[Race.BLACK]),
            ],
            resolution_strategy_config=_DEFAULT_CONFIG,
        )
        assert resolved is not None
        self.assertEqual(
            {race.race for race in resolved.races}, {Race.WHITE, Race.BLACK}
        )

    def test_list_fields_dedup_equal_elements(self) -> None:
        # The same value recorded on two fragments collapses to one; the union
        # dedups by value equality rather than duplicating it.
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _fragment(races=[Race.WHITE]),
                _fragment(races=[Race.WHITE]),
            ],
            resolution_strategy_config=_DEFAULT_CONFIG,
        )
        assert resolved is not None
        self.assertEqual([race.race for race in resolved.races], [Race.WHITE])

    def test_list_fields_dedup_by_semantic_key_keeping_latest_raw_text(self) -> None:
        # Two fragments record the same race with different raw text. They
        # collapse to one race carrying the latest fragment's raw text.
        def _race_fragment(external_id: str, race_raw_text: str) -> IdentityFragment:
            return IdentityFragment(
                tenant=_TENANT,
                external_ids=[
                    IdentityExternalId(
                        tenant=_TENANT, external_id=external_id, id_type="US_OZ_T1"
                    )
                ],
                person_type=PersonType.JII,
                attributes=IdentityAttributes(
                    tenant=_TENANT,
                    races=[
                        IdentityRace(
                            tenant=_TENANT,
                            race=Race.BLACK,
                            race_raw_text=race_raw_text,
                        )
                    ],
                ),
            )

        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                _race_fragment("A", "B"),
                _race_fragment("B", "BLACK"),
            ],
            resolution_strategy_config=_DEFAULT_CONFIG,
        )
        assert resolved is not None
        self.assertEqual(len(resolved.races), 1)
        self.assertEqual(resolved.races[0].race, Race.BLACK)
        self.assertEqual(resolved.races[0].race_raw_text, "BLACK")

    def test_all_fields_null_resolved_returns_none(self) -> None:
        # The only attribute is a divergent optional attribute that nulls out,
        # so nothing remains to store.
        self.assertIsNone(
            resolve_cluster_attributes(
                tenant=_TENANT,
                sorted_fragments=[
                    _fragment(gender=Gender.MALE),
                    _fragment(gender=Gender.FEMALE),
                ],
                resolution_strategy_config=_DEFAULT_CONFIG,
            )
        )


class TestResolveClusterAttributesFieldCoverage(unittest.TestCase):
    """Guards that resolve_cluster_attributes keeps up with the entity schema.

    The resolution logic names each field it resolves, so a field added to
    IdentityAttributes or IdentityName would otherwise be silently dropped
    from every kept cluster. When one of these tests fails, wire the new field
    into resolve_cluster_attributes (or exempt it here if it is not resolved
    across fragments) and update the expected set.

    The schema tests below account for every entity field; the behavioral test
    proves each field the schema tests call "resolved" is actually produced by
    resolve_cluster_attributes, so dropping a `_resolve_scalar`/`_union`/
    `_latest` call fails a test rather than silently dropping the field.
    """

    def test_every_resolved_field_is_wired(self) -> None:
        """Two fragments differ on every scalar and contribute distinct list
        elements. With latest-wins resolution the resolved entity must carry
        the later scalar for every field and the unioned lists; a dropped
        resolution call surfaces as a None/empty field here."""
        early = IdentityAttributes(
            tenant=_TENANT,
            name=IdentityName(
                tenant=_TENANT,
                given_name="JOHN",
                middle_name="ALAN",
                surname="SMITH",
                name_suffix="JR",
                preferred_name="JOHNNY",
            ),
            birthdate=datetime.date(1990, 1, 1),
            sex=IdentitySex(tenant=_TENANT, sex=Sex.MALE),
            gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
            ethnicity=IdentityEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
            races=[IdentityRace(tenant=_TENANT, race=Race.WHITE)],
            phone_numbers=[IdentityPhoneNumber(tenant=_TENANT, number="5550100001")],
            emails=[IdentityEmail(tenant=_TENANT, address="john@example.com")],
        )
        late = IdentityAttributes(
            tenant=_TENANT,
            name=IdentityName(
                tenant=_TENANT,
                given_name="JON",
                middle_name="BILL",
                surname="SMYTHE",
                name_suffix="SR",
                preferred_name="JONNY",
            ),
            birthdate=datetime.date(1991, 2, 2),
            sex=IdentitySex(tenant=_TENANT, sex=Sex.FEMALE),
            gender=IdentityGender(tenant=_TENANT, gender=Gender.FEMALE),
            ethnicity=IdentityEthnicity(
                tenant=_TENANT, ethnicity=Ethnicity.NOT_HISPANIC
            ),
            races=[IdentityRace(tenant=_TENANT, race=Race.BLACK)],
            phone_numbers=[IdentityPhoneNumber(tenant=_TENANT, number="5550100002")],
            emails=[IdentityEmail(tenant=_TENANT, address="jane@example.com")],
        )
        # Latest for every conflict-checked scalar, so each resolves to the late
        # fragment's value.
        latest_config = {
            a: ResolutionStrategy.KEEP_LATEST for a in ConflictCheckedAttribute
        }
        resolved = resolve_cluster_attributes(
            tenant=_TENANT,
            sorted_fragments=[
                IdentityFragment(
                    tenant=_TENANT,
                    external_ids=[
                        IdentityExternalId(
                            tenant=_TENANT, external_id="A", id_type="US_OZ_T1"
                        )
                    ],
                    person_type=PersonType.JII,
                    attributes=attributes,
                )
                for attributes in (early, late)
            ],
            resolution_strategy_config=latest_config,
        )
        assert resolved is not None and resolved.name is not None
        self.assertEqual(resolved.name.given_name, "JON")
        self.assertEqual(resolved.name.middle_name, "BILL")
        self.assertEqual(resolved.name.surname, "SMYTHE")
        self.assertEqual(resolved.name.name_suffix, "SR")
        self.assertEqual(resolved.name.preferred_name, "JONNY")
        self.assertEqual(resolved.birthdate, datetime.date(1991, 2, 2))
        assert resolved.sex and resolved.gender and resolved.ethnicity
        self.assertEqual(resolved.sex.sex, Sex.FEMALE)
        self.assertEqual(resolved.gender.gender, Gender.FEMALE)
        self.assertEqual(resolved.ethnicity.ethnicity, Ethnicity.NOT_HISPANIC)
        self.assertEqual({r.race for r in resolved.races}, {Race.WHITE, Race.BLACK})
        self.assertEqual(
            {p.number for p in resolved.phone_numbers}, {"5550100001", "5550100002"}
        )
        self.assertEqual(
            {e.address for e in resolved.emails},
            {"john@example.com", "jane@example.com"},
        )

    def test_identity_attributes_fields_all_resolved_or_exempt(self) -> None:
        resolved_fields = {
            "name",
            "birthdate",
            "sex",
            "gender",
            "ethnicity",
            "races",
            "phone_numbers",
            "emails",
        }
        exempt_fields = {
            # Set directly on the resolved entity, not resolved across
            # fragments.
            "tenant",
            # Back-edge to the owning fragment; the resolved attributes belong
            # to the cluster.
            "fragment",
        }
        self.assertEqual(
            {f.name for f in attr.fields(IdentityAttributes)},
            resolved_fields | exempt_fields,
        )

    def test_identity_name_fields_all_resolved_or_exempt(self) -> None:
        resolved_fields = {
            "given_name",
            "middle_name",
            "surname",
            "name_suffix",
            "preferred_name",
        }
        exempt_fields = {
            # Set directly on the resolved entity, not resolved across
            # fragments.
            "tenant",
            # Back-edge to the owning attributes entity.
            "identity_attributes",
        }
        self.assertEqual(
            {f.name for f in attr.fields(IdentityName)},
            resolved_fields | exempt_fields,
        )
