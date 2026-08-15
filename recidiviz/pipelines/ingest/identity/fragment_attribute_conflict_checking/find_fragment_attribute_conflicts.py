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
"""Finds the attribute conflicts within a cluster."""
import datetime
from itertools import combinations

import attr

from recidiviz.common import attr_validators
from recidiviz.common.demographics import Ethnicity, Gender, Sex
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.alias_explanation import (
    aliases_explain_name_component_conflict,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.name import (
    Name,
    NameComponent,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.signals import (
    are_dobs_in_conflict,
    are_given_names_in_conflict,
    are_middle_names_in_conflict,
    are_name_suffixes_in_conflict,
    are_surnames_in_conflict,
    names_match_exactly,
    names_match_loosely,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
    ConflictCheckedAttributesConfig,
    OptionalConflictCheckedAttribute,
)
from recidiviz.pipelines.ingest.identity.types import AttributeConflict, ConflictValue


@attr.define(frozen=True, kw_only=True)
class _FragmentValues:
    """The conflict-checked values pulled off one fragment. Every field is
    None-safe, so an external-id-only fragment yields all None.

    The fields are named after the ConflictCheckedAttribute values and read back
    by that name, so the config and the reported field names cannot drift apart.
    """

    surname: str | None = attr.ib(validator=attr_validators.is_opt_str)
    given_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    middle_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    name_suffix: str | None = attr.ib(validator=attr_validators.is_opt_str)
    birthdate: datetime.date | None = attr.ib(validator=attr_validators.is_opt_date)
    sex: Sex | None = attr.ib(validator=attr_validators.is_opt(Sex))
    gender: Gender | None = attr.ib(validator=attr_validators.is_opt(Gender))
    ethnicity: Ethnicity | None = attr.ib(validator=attr_validators.is_opt(Ethnicity))

    @property
    def name(self) -> Name:
        """Returns the primary name components as one whole Name."""
        return Name(
            surname=self.surname,
            given_name=self.given_name,
            middle_name=self.middle_name,
            name_suffix=self.name_suffix,
        )

    @classmethod
    def from_fragment(cls, fragment: IdentityFragment) -> "_FragmentValues":
        """Pulls the conflict-checked values off a fragment, tolerating a
        fragment (or sub-entity) that carries none of them."""
        attributes = fragment.attributes
        name = attributes.name if attributes else None
        return cls(
            surname=name.surname if name else None,
            given_name=name.given_name if name else None,
            middle_name=name.middle_name if name else None,
            name_suffix=name.name_suffix if name else None,
            birthdate=attributes.birthdate if attributes else None,
            sex=attributes.sex.sex if attributes and attributes.sex else None,
            gender=(
                attributes.gender.gender if attributes and attributes.gender else None
            ),
            ethnicity=(
                attributes.ethnicity.ethnicity
                if attributes and attributes.ethnicity
                else None
            ),
        )


@attr.define(frozen=True, kw_only=True)
class _FragmentAliases:
    """The alias names pulled off one fragment, each kept whole as a
    Name. Keeping the row together is what lets an alias be matched as a
    name rather than as a bag of interchangeable components, so aliases of
    "MARIA GONZALEZ" and "JOHN SMITH" never combine into a "MARIA SMITH" the
    source never recorded. Empty for a fragment that records no aliases."""

    names: tuple[Name, ...] = attr.ib(validator=attr_validators.is_tuple_of(Name))

    @classmethod
    def from_fragment(cls, fragment: IdentityFragment) -> "_FragmentAliases":
        """Pulls the alias names off a fragment, tolerating a fragment that
        records no aliases."""
        aliases = fragment.attributes.aliases if fragment.attributes else []
        return cls(
            names=tuple(
                Name(
                    surname=alias.surname,
                    given_name=alias.given_name,
                    middle_name=alias.middle_name,
                    name_suffix=alias.name_suffix,
                )
                for alias in aliases
            )
        )


def find_fragment_attribute_conflicts(
    fragments: list[IdentityFragment],
    conflict_checked_attributes_config: ConflictCheckedAttributesConfig,
) -> tuple[AttributeConflict, ...]:
    """Returns an AttributeConflict for each attribute the given fragments
    conflict on, or an empty tuple when they do not conflict.

    Conflict is decided across every pair of fragments. Nulls never conflict, so
    external-id-only fragments and sparse sources pass trivially. Values are
    reported in the order the fragments are given.

    Args:
        fragments: The cluster's fragments to check against one another.
        conflict_checked_attributes_config: Whether each optional attribute (sex,
            gender, ethnicity) is conflict-checked for this tenant.
    """
    fragment_values = [_FragmentValues.from_fragment(f) for f in fragments]
    fragment_aliases = [_FragmentAliases.from_fragment(f) for f in fragments]

    fields_in_conflict: set[ConflictCheckedAttribute] = set()
    for (a, aliases_a), (b, aliases_b) in combinations(
        zip(fragment_values, fragment_aliases), 2
    ):
        surnames_match_exactly = names_match_exactly(a.surname, b.surname)
        given_names_match_exactly = names_match_exactly(a.given_name, b.given_name)
        given_names_match_loosely = names_match_loosely(a.given_name, b.given_name)
        dobs_match_exactly = a.birthdate is not None and a.birthdate == b.birthdate

        if are_surnames_in_conflict(
            a.surname,
            b.surname,
            given_names_match_loosely=given_names_match_loosely,
            dobs_match_exactly=dobs_match_exactly,
        ) and not aliases_explain_name_component_conflict(
            name_a=a.name,
            name_b=b.name,
            conflicting_component=NameComponent.SURNAME,
            aliases_a=aliases_a.names,
            aliases_b=aliases_b.names,
        ):
            fields_in_conflict.add(ConflictCheckedAttribute.SURNAME)
        if are_given_names_in_conflict(
            a.given_name,
            b.given_name,
            surnames_match_exactly=surnames_match_exactly,
            dobs_match_exactly=dobs_match_exactly,
        ) and not aliases_explain_name_component_conflict(
            name_a=a.name,
            name_b=b.name,
            conflicting_component=NameComponent.GIVEN_NAME,
            aliases_a=aliases_a.names,
            aliases_b=aliases_b.names,
        ):
            fields_in_conflict.add(ConflictCheckedAttribute.GIVEN_NAME)
        if are_middle_names_in_conflict(
            a.middle_name,
            b.middle_name,
            surnames_match_exactly=surnames_match_exactly,
            dobs_match_exactly=dobs_match_exactly,
        ) and not aliases_explain_name_component_conflict(
            name_a=a.name,
            name_b=b.name,
            conflicting_component=NameComponent.MIDDLE_NAME,
            aliases_a=aliases_a.names,
            aliases_b=aliases_b.names,
        ):
            fields_in_conflict.add(ConflictCheckedAttribute.MIDDLE_NAME)
        if are_name_suffixes_in_conflict(a.name_suffix, b.name_suffix):
            fields_in_conflict.add(ConflictCheckedAttribute.NAME_SUFFIX)
        if are_dobs_in_conflict(
            a.birthdate,
            b.birthdate,
            surnames_and_given_names_match_exactly=(
                surnames_match_exactly and given_names_match_exactly
            ),
        ):
            fields_in_conflict.add(ConflictCheckedAttribute.BIRTHDATE)

        # sex, gender, and ethnicity are conflict-checked only when the tenant's
        # config says so. When they are, any recorded disagreement is a conflict.
        for optional_attribute in OptionalConflictCheckedAttribute:
            if not conflict_checked_attributes_config[optional_attribute]:
                continue
            attribute = ConflictCheckedAttribute(optional_attribute.value)
            value_a = getattr(a, attribute.value)
            value_b = getattr(b, attribute.value)
            if value_a is not None and value_b is not None and value_a != value_b:
                fields_in_conflict.add(attribute)

    return tuple(
        AttributeConflict(
            field=attribute.value,
            values=_distinct_values(fragment_values, attribute),
        )
        for attribute in ConflictCheckedAttribute
        if attribute in fields_in_conflict
    )


def _distinct_values(
    fragment_values: list[_FragmentValues], attribute: ConflictCheckedAttribute
) -> tuple[ConflictValue, ...]:
    """Returns the distinct non-null values recorded for an attribute across the
    cluster's fragments, in first-seen order."""
    distinct: list[ConflictValue] = []
    for values in fragment_values:
        value = getattr(values, attribute.value)
        if value is not None and value not in distinct:
            distinct.append(value)
    return tuple(distinct)
