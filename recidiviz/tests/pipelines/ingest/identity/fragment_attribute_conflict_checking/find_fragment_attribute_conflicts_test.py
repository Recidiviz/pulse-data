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
"""Tests for find_fragment_attribute_conflicts."""
import datetime
import unittest

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Sex
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityEthnicity,
    IdentityExternalId,
    IdentityFragment,
    IdentityGender,
    IdentityName,
    IdentitySex,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.find_fragment_attribute_conflicts import (
    _FragmentValues,
    find_fragment_attribute_conflicts,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    IdentityIngestPipelineTenantConfig,
    OptionalConflictCheckedAttribute,
)
from recidiviz.pipelines.ingest.identity.types import AttributeConflict

_TENANT = Tenant.US_OZ
_DEFAULT_CONFIG = (
    IdentityIngestPipelineTenantConfig().conflict_checked_attributes_config
)
_SEX_OFF_CONFIG = {**_DEFAULT_CONFIG, OptionalConflictCheckedAttribute.SEX: False}
_GENDER_ON_CONFIG = {**_DEFAULT_CONFIG, OptionalConflictCheckedAttribute.GENDER: True}


def _fragment(
    *,
    external_id: str = "A",
    surname: str | None = None,
    given_name: str | None = None,
    middle_name: str | None = None,
    name_suffix: str | None = None,
    birthdate: datetime.date | None = None,
    sex: Sex | None = None,
    gender: Gender | None = None,
    ethnicity: Ethnicity | None = None,
) -> IdentityFragment:
    """Builds a JII fragment carrying the given attributes; an all-None call
    yields an external-id-only fragment with no attributes."""
    name = (
        IdentityName(
            tenant=_TENANT,
            surname=surname,
            given_name=given_name,
            middle_name=middle_name,
            name_suffix=name_suffix,
        )
        if surname or given_name or middle_name or name_suffix
        else None
    )
    attributes = (
        IdentityAttributes(
            tenant=_TENANT,
            name=name,
            birthdate=birthdate,
            sex=IdentitySex(tenant=_TENANT, sex=sex) if sex else None,
            gender=IdentityGender(tenant=_TENANT, gender=gender) if gender else None,
            ethnicity=(
                IdentityEthnicity(tenant=_TENANT, ethnicity=ethnicity)
                if ethnicity
                else None
            ),
        )
        if (name or birthdate or sex or gender or ethnicity)
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


class TestFindFragmentAttributeConflicts(unittest.TestCase):
    """Tests for find_fragment_attribute_conflicts."""

    def test_single_fragment_never_conflicts(self) -> None:
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [_fragment(surname="SMITH", given_name="JOHN")], _DEFAULT_CONFIG
            ),
            (),
        )

    def test_identical_fragments_do_not_conflict(self) -> None:
        frag: dict = {
            "surname": "SMITH",
            "given_name": "JOHN",
            "birthdate": datetime.date(1990, 1, 1),
        }
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [_fragment(**frag), _fragment(**frag)], _DEFAULT_CONFIG
            ),
            (),
        )

    def test_external_id_only_fragments_do_not_conflict(self) -> None:
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [_fragment(external_id="A"), _fragment(external_id="B")],
                _DEFAULT_CONFIG,
            ),
            (),
        )

    def test_surname_beyond_threshold_conflicts(self) -> None:
        conflicts = find_fragment_attribute_conflicts(
            [_fragment(surname="WILLIAMS"), _fragment(surname="JOHNSON")],
            _DEFAULT_CONFIG,
        )
        self.assertEqual(
            conflicts,
            (AttributeConflict(field="surname", values=("WILLIAMS", "JOHNSON")),),
        )

    def test_surname_within_threshold_does_not_conflict(self) -> None:
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [_fragment(surname="GUSTAFSON"), _fragment(surname="GUSTAFSEN")],
                _DEFAULT_CONFIG,
            ),
            (),
        )

    def test_surname_name_change_hatch(self) -> None:
        # Different surnames, but same given name and exact DOB: a name change.
        birthdate = datetime.date(1990, 1, 1)
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [
                    _fragment(
                        surname="WILLIAMS", given_name="JANE", birthdate=birthdate
                    ),
                    _fragment(
                        surname="JOHNSON", given_name="JANE", birthdate=birthdate
                    ),
                ],
                _DEFAULT_CONFIG,
            ),
            (),
        )

    def test_given_name_nickname_corroborated_does_not_conflict(self) -> None:
        birthdate = datetime.date(1990, 1, 1)
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [
                    _fragment(surname="SMITH", given_name="BOB", birthdate=birthdate),
                    _fragment(
                        surname="SMITH", given_name="ROBERT", birthdate=birthdate
                    ),
                ],
                _DEFAULT_CONFIG,
            ),
            (),
        )

    def test_given_name_nickname_uncorroborated_conflicts(self) -> None:
        # Same surname but no DOB to corroborate, so the nickname is not trusted.
        conflicts = find_fragment_attribute_conflicts(
            [
                _fragment(surname="SMITH", given_name="BOB"),
                _fragment(surname="SMITH", given_name="ROBERT"),
            ],
            _DEFAULT_CONFIG,
        )
        self.assertEqual(
            conflicts,
            (AttributeConflict(field="given_name", values=("BOB", "ROBERT")),),
        )

    def test_dob_typo_does_not_conflict(self) -> None:
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [
                    _fragment(birthdate=datetime.date(1990, 1, 1)),
                    _fragment(birthdate=datetime.date(1990, 1, 2)),
                ],
                _DEFAULT_CONFIG,
            ),
            (),
        )

    def test_dob_genuinely_different_conflicts(self) -> None:
        conflicts = find_fragment_attribute_conflicts(
            [
                _fragment(birthdate=datetime.date(1990, 1, 1)),
                _fragment(birthdate=datetime.date(1985, 6, 15)),
            ],
            _DEFAULT_CONFIG,
        )
        self.assertEqual(
            conflicts,
            (
                AttributeConflict(
                    field="birthdate",
                    values=(datetime.date(1990, 1, 1), datetime.date(1985, 6, 15)),
                ),
            ),
        )

    def test_dob_year_slip_excused_when_names_vouch(self) -> None:
        # A single mistyped year digit moves the birthdate a decade; excused
        # because the surname and given name both match exactly.
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [
                    _fragment(
                        surname="SMITH",
                        given_name="JOHN",
                        birthdate=datetime.date(1990, 1, 1),
                    ),
                    _fragment(
                        surname="SMITH",
                        given_name="JOHN",
                        birthdate=datetime.date(1980, 1, 1),
                    ),
                ],
                _DEFAULT_CONFIG,
            ),
            (),
        )

    def test_dob_year_slip_without_name_vouch_conflicts(self) -> None:
        # The same decade slip with no given name to vouch for the pair is a
        # genuine conflict.
        conflicts = find_fragment_attribute_conflicts(
            [
                _fragment(surname="SMITH", birthdate=datetime.date(1990, 1, 1)),
                _fragment(surname="SMITH", birthdate=datetime.date(1980, 1, 1)),
            ],
            _DEFAULT_CONFIG,
        )
        self.assertEqual(
            conflicts,
            (
                AttributeConflict(
                    field="birthdate",
                    values=(datetime.date(1990, 1, 1), datetime.date(1980, 1, 1)),
                ),
            ),
        )

    def test_sex_conflicts_by_default(self) -> None:
        conflicts = find_fragment_attribute_conflicts(
            [_fragment(sex=Sex.MALE), _fragment(sex=Sex.FEMALE)], _DEFAULT_CONFIG
        )
        self.assertEqual(
            conflicts,
            (AttributeConflict(field="sex", values=(Sex.MALE, Sex.FEMALE)),),
        )

    def test_sex_divergence_ignored_when_check_off(self) -> None:
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [_fragment(sex=Sex.MALE), _fragment(sex=Sex.FEMALE)], _SEX_OFF_CONFIG
            ),
            (),
        )

    def test_gender_divergence_not_a_conflict_by_default(self) -> None:
        self.assertEqual(
            find_fragment_attribute_conflicts(
                [_fragment(gender=Gender.MALE), _fragment(gender=Gender.FEMALE)],
                _DEFAULT_CONFIG,
            ),
            (),
        )

    def test_gender_conflict_when_check_enabled(self) -> None:
        conflicts = find_fragment_attribute_conflicts(
            [_fragment(gender=Gender.MALE), _fragment(gender=Gender.FEMALE)],
            _GENDER_ON_CONFIG,
        )
        self.assertEqual(
            conflicts,
            (AttributeConflict(field="gender", values=(Gender.MALE, Gender.FEMALE)),),
        )

    def test_any_conflicting_pair_rejects_cluster(self) -> None:
        # Two fragments agree; a third brings a genuinely different surname, so
        # the whole cluster is in conflict.
        conflicts = find_fragment_attribute_conflicts(
            [
                _fragment(external_id="A", surname="SMITH"),
                _fragment(external_id="B", surname="SMITH"),
                _fragment(external_id="C", surname="JOHNSON"),
            ],
            _DEFAULT_CONFIG,
        )
        self.assertEqual(
            conflicts,
            (AttributeConflict(field="surname", values=("SMITH", "JOHNSON")),),
        )

    def test_multiple_fields_conflict(self) -> None:
        conflicts = find_fragment_attribute_conflicts(
            [
                _fragment(surname="WILLIAMS", sex=Sex.MALE),
                _fragment(surname="JOHNSON", sex=Sex.FEMALE),
            ],
            _DEFAULT_CONFIG,
        )
        self.assertEqual(
            conflicts,
            (
                AttributeConflict(field="surname", values=("WILLIAMS", "JOHNSON")),
                AttributeConflict(field="sex", values=(Sex.MALE, Sex.FEMALE)),
            ),
        )


class TestConflictCheckFieldCoverage(unittest.TestCase):
    """Guards that the conflict check keeps up with the entity schema.

    find_fragment_attribute_conflicts checks exactly the fields pulled into
    _FragmentValues, so a field added to IdentityAttributes or IdentityName
    would otherwise silently escape conflict checking. When this test fails,
    either pull the new field into _FragmentValues and check it, or exempt it
    here with a reason.
    """

    def test_entity_fields_all_checked_or_exempt(self) -> None:
        checked_fields = {f.name for f in attr.fields(_FragmentValues)}
        exempt_fields = {
            # Set on every entity, never compared across fragments.
            "tenant",
            # Back-edges.
            "fragment",
            "identity_attributes",
            # Checked component-wise via the IdentityName fields.
            "name",
            # Free-form and not identity-discriminating.
            "preferred_name",
            # List-valued attributes union across fragments, so disagreement
            # between fragments is expected rather than a conflict signal.
            "races",
            "phone_numbers",
            "emails",
        }
        entity_fields = {f.name for f in attr.fields(IdentityAttributes)} | {
            f.name for f in attr.fields(IdentityName)
        }
        self.assertEqual(checked_fields | exempt_fields, entity_fields)
        self.assertFalse(checked_fields & exempt_fields)

    def test_optional_conflict_checked_attributes_name_checked_fields(self) -> None:
        """find_fragment_attribute_conflicts reads each optional attribute with
        getattr by the enum's value, so every OptionalConflictCheckedAttribute
        value must name a _FragmentValues field."""
        checked_fields = {f.name for f in attr.fields(_FragmentValues)}
        self.assertLessEqual(
            {a.value for a in OptionalConflictCheckedAttribute}, checked_fields
        )
