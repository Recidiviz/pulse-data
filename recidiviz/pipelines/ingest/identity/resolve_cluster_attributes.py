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
"""Resolves a kept cluster's fragments into one IdentityAttributes.

The conflict checks have already accepted the cluster as one person, so any
remaining scalar divergence is benign. Each conflict-checked scalar resolves per
the tenant's resolution strategy, either keeping the latest value or storing
nothing when its fragments diverge. preferred_name is not conflict-checked and
always takes the latest value. List-valued attributes union across fragments.
Fragments must be sorted oldest-first by (upper_bound_date, ingest view) so the
latest value is last.
"""
from collections.abc import Callable
from typing import TypeVar

from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityFragment,
    IdentityName,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.signals import (
    canonical_suffix,
    normalize_name,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
    ResolutionStrategy,
    ResolutionStrategyConfig,
)

_SourceT = TypeVar("_SourceT")
_ValueT = TypeVar("_ValueT")


def resolve_cluster_attributes(
    *,
    tenant: Tenant,
    sorted_fragments: list[IdentityFragment],
    resolution_strategy_config: ResolutionStrategyConfig,
) -> IdentityAttributes | None:
    """Returns the single IdentityAttributes a kept cluster stores, or None if
    its fragments resolve to no attributes at all."""
    attributes = [f.attributes for f in sorted_fragments if f.attributes is not None]
    if not attributes:
        return None

    name = _resolve_name(tenant, attributes, resolution_strategy_config)
    birthdate = _resolve_scalar(
        attributes,
        lambda a: a.birthdate,
        resolution_strategy_config[ConflictCheckedAttribute.BIRTHDATE],
    )
    sex = _resolve_scalar(
        attributes,
        lambda a: a.sex,
        resolution_strategy_config[ConflictCheckedAttribute.SEX],
        key=lambda s: s.sex,
    )
    gender = _resolve_scalar(
        attributes,
        lambda a: a.gender,
        resolution_strategy_config[ConflictCheckedAttribute.GENDER],
        key=lambda g: g.gender,
    )
    ethnicity = _resolve_scalar(
        attributes,
        lambda a: a.ethnicity,
        resolution_strategy_config[ConflictCheckedAttribute.ETHNICITY],
        key=lambda e: e.ethnicity,
    )
    races = _union(attributes, lambda a: a.races, key=lambda r: r.race)
    phone_numbers = _union(
        attributes, lambda a: a.phone_numbers, key=lambda p: p.number
    )
    emails = _union(attributes, lambda a: a.emails, key=lambda e: e.address)
    aliases = _union(
        attributes,
        lambda a: a.aliases,
        key=lambda al: (
            al.given_name,
            al.surname,
            al.middle_name,
            al.name_suffix,
            al.name_use,
        ),
    )

    if not any(
        [
            name,
            birthdate,
            sex,
            gender,
            ethnicity,
            races,
            phone_numbers,
            emails,
            aliases,
        ]
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


def _resolve_name(
    tenant: Tenant,
    attributes: list[IdentityAttributes],
    resolution_strategy_config: ResolutionStrategyConfig,
) -> IdentityName | None:
    """Resolves each name component per the tenant's resolution strategy; the
    components may come from different fragments. preferred_name is not
    conflict-checked, so it always takes the latest value."""
    names = [a.name for a in attributes if a.name is not None]
    if not names:
        return None
    given_name = _resolve_scalar(
        names,
        lambda n: n.given_name,
        resolution_strategy_config[ConflictCheckedAttribute.GIVEN_NAME],
        key=normalize_name,
    )
    middle_name = _resolve_scalar(
        names,
        lambda n: n.middle_name,
        resolution_strategy_config[ConflictCheckedAttribute.MIDDLE_NAME],
        key=normalize_name,
    )
    surname = _resolve_scalar(
        names,
        lambda n: n.surname,
        resolution_strategy_config[ConflictCheckedAttribute.SURNAME],
        key=normalize_name,
    )
    name_suffix = _resolve_scalar(
        names,
        lambda n: n.name_suffix,
        resolution_strategy_config[ConflictCheckedAttribute.NAME_SUFFIX],
        key=canonical_suffix,
    )
    preferred_name = _latest(names, lambda n: n.preferred_name)
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


def _resolve_scalar(
    sources: list[_SourceT],
    getter: Callable[[_SourceT], _ValueT | None],
    resolution: ResolutionStrategy,
    key: Callable[[_ValueT], object] | None = None,
) -> _ValueT | None:
    """Returns the latest non-null value across the oldest-first sources, or None
    when the values diverge and the resolution strategy is SET_NULL.

    The key maps a value to the identity its divergence is judged on, mirroring
    how the conflict checker decides two values record the same thing. Name
    components use their normalized form, so casing, accent, and punctuation
    variants of one name do not count as divergent. Name suffixes use their
    canonical form, so JR and Junior do not count as divergent. The enum entities
    use their wrapped enum, because entity equality also compares raw-text fields
    and equal enums with different raw text would otherwise count as divergent.
    Dates compare as themselves.
    """
    values = [value for s in sources if (value := getter(s)) is not None]
    if not values:
        return None
    distinct: set[object] = {
        key(value) if key is not None else value for value in values
    }
    if len(distinct) > 1 and resolution is ResolutionStrategy.SET_NULL:
        return None
    return values[-1]


def _union(
    attributes: list[IdentityAttributes],
    getter: Callable[[IdentityAttributes], list[_ValueT]],
    key: Callable[[_ValueT], object],
) -> list[_ValueT]:
    """Returns the deduplicated union of a list-valued attribute across all
    fragments, keeping the latest representative for each key (as _resolve_scalar
    keeps the latest scalar value).

    The key maps an item to the identity it is deduplicated on, so two items
    recording the same fact with different raw text (IdentityRace(race=BLACK,
    race_raw_text="B") and IdentityRace(race=BLACK, race_raw_text="BLACK"))
    collapse to one carrying the latest fragment's raw text. Relies on attributes
    being ordered oldest-first.
    """
    latest_by_key: dict[object, _ValueT] = {}
    for attribute in attributes:
        for item in getter(attribute):
            latest_by_key[key(item)] = item
    return list(latest_by_key.values())


def _latest(
    sources: list[_SourceT], getter: Callable[[_SourceT], _ValueT | None]
) -> _ValueT | None:
    """Returns the last non-null value the getter produces across the oldest-first
    sources (the latest one)."""
    latest: _ValueT | None = None
    for source in sources:
        value = getter(source)
        if value is not None:
            latest = value
    return latest
