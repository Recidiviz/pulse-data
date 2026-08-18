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
"""Builds the attributes a blessed cluster stores by overlaying a reviewer's
recorded values onto the pipeline's normal resolution.

A BLESS override keeps a cluster whose fragments conflict. Each attribute the
cluster conflicts on takes the override's recorded value, or null when the
reviewer left it unset, so the pipeline never picks a winner for an attribute a
human has already judged. Every other attribute resolves normally from the
fragments.

For example, a cluster whose fragments disagree on surname (WILLIAMS vs JOHNSON)
but agree on the given name JOHN, blessed with surname RIVERA, keeps given name
JOHN from resolution and takes surname RIVERA from the override. Blessed with no
surname, it keeps given name JOHN and stores a null surname.
"""
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityEthnicity,
    IdentityGender,
    IdentityName,
    IdentitySex,
)
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    BlessedIdentityValues,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
)


def build_blessed_cluster_attributes(
    *,
    tenant: Tenant,
    resolved_attributes: IdentityAttributes | None,
    blessed_values: BlessedIdentityValues,
    conflicting_attributes: set[ConflictCheckedAttribute],
) -> IdentityAttributes | None:
    """Returns the attributes a blessed cluster stores, or None when they are all
    empty. Each conflicting attribute takes the reviewer's recorded value, or null
    when the reviewer recorded none; every non-conflicting attribute keeps its
    normally-resolved value."""
    resolved_name = (
        resolved_attributes.name if resolved_attributes is not None else None
    )
    name = _build_blessed_name(
        tenant=tenant,
        resolved_name=resolved_name,
        blessed_values=blessed_values,
        conflicting_attributes=conflicting_attributes,
    )
    birthdate = (
        blessed_values.birthdate
        if ConflictCheckedAttribute.BIRTHDATE in conflicting_attributes
        else (resolved_attributes.birthdate if resolved_attributes else None)
    )
    sex = (
        (
            IdentitySex(tenant=tenant, sex=blessed_values.sex)
            if blessed_values.sex is not None
            else None
        )
        if ConflictCheckedAttribute.SEX in conflicting_attributes
        else (resolved_attributes.sex if resolved_attributes else None)
    )
    gender = (
        (
            IdentityGender(tenant=tenant, gender=blessed_values.gender)
            if blessed_values.gender is not None
            else None
        )
        if ConflictCheckedAttribute.GENDER in conflicting_attributes
        else (resolved_attributes.gender if resolved_attributes else None)
    )
    ethnicity = (
        (
            IdentityEthnicity(tenant=tenant, ethnicity=blessed_values.ethnicity)
            if blessed_values.ethnicity is not None
            else None
        )
        if ConflictCheckedAttribute.ETHNICITY in conflicting_attributes
        else (resolved_attributes.ethnicity if resolved_attributes else None)
    )
    races = resolved_attributes.races if resolved_attributes else []
    phone_numbers = resolved_attributes.phone_numbers if resolved_attributes else []
    emails = resolved_attributes.emails if resolved_attributes else []
    aliases = resolved_attributes.aliases if resolved_attributes else []
    if not any(
        [name, birthdate, sex, gender, ethnicity, races, phone_numbers, emails, aliases]
    ):
        return None
    return IdentityAttributes(
        tenant=tenant,
        name=name,
        birthdate=birthdate,
        sex=sex,
        gender=gender,
        ethnicity=ethnicity,
        races=races,
        phone_numbers=phone_numbers,
        emails=emails,
        aliases=aliases,
    )


def _build_blessed_name(
    *,
    tenant: Tenant,
    resolved_name: IdentityName | None,
    blessed_values: BlessedIdentityValues,
    conflicting_attributes: set[ConflictCheckedAttribute],
) -> IdentityName | None:
    """Returns the name a blessed cluster stores, taking each conflicting name
    component from the blessed values (which may be null) and every other
    component from the resolved name. preferred_name is never conflict-checked, so
    it always comes from resolution. Returns None when no component has a value."""
    given_name = (
        blessed_values.given_name
        if ConflictCheckedAttribute.GIVEN_NAME in conflicting_attributes
        else (resolved_name.given_name if resolved_name else None)
    )
    middle_name = (
        blessed_values.middle_name
        if ConflictCheckedAttribute.MIDDLE_NAME in conflicting_attributes
        else (resolved_name.middle_name if resolved_name else None)
    )
    surname = (
        blessed_values.surname
        if ConflictCheckedAttribute.SURNAME in conflicting_attributes
        else (resolved_name.surname if resolved_name else None)
    )
    name_suffix = (
        blessed_values.name_suffix
        if ConflictCheckedAttribute.NAME_SUFFIX in conflicting_attributes
        else (resolved_name.name_suffix if resolved_name else None)
    )
    preferred_name = resolved_name.preferred_name if resolved_name else None
    if not any([given_name, middle_name, surname, name_suffix, preferred_name]):
        return None
    return IdentityName(
        tenant=tenant,
        given_name=given_name,
        middle_name=middle_name,
        surname=surname,
        name_suffix=name_suffix,
        preferred_name=preferred_name,
    )
