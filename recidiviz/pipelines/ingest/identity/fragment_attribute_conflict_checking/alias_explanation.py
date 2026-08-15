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
"""Decides whether a recorded alias explains a name conflict between two
fragments.

The conflict rules in signals give the per-attribute verdicts, layering
corroboration excuses over raw name comparisons. This module layers one more
excuse over the name verdicts, keeping a pair together when either fragment
records an alias that is the other fragment's primary name.
"""
from collections.abc import Iterable

from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.name import (
    Name,
    NameComponent,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.signals import (
    are_name_suffixes_in_conflict,
    is_initial_match,
    names_match_loosely,
    normalize_name,
)


def aliases_explain_name_component_conflict(
    *,
    name_a: Name,
    name_b: Name,
    conflicting_component: NameComponent,
    aliases_a: Iterable[Name],
    aliases_b: Iterable[Name],
) -> bool:
    """Returns whether a recorded alias explains the conflict between two
    fragments' primary names on one component, that is, whether either fragment
    records an alias that matches the other fragment's primary name. This keeps
    a pair together when one source records a maiden name or nickname that the
    other uses as the primary name.

    An alias is matched as a whole name, not as a bag of components. The alias
    must loosely match on the conflicting component and agree with the primary
    name on every other component both of them record, so an alias of
    "MARIA GONZALEZ" excuses a surname conflict against "MARIA SMITH" but not
    against "JOHN SMITH".

    Only primary-vs-alias matches count. Two fragments that merely share an
    alias, with primary names that match neither, stay in conflict: alias-to-
    alias agreement is weaker evidence that the fragments describe one person.
    """
    return _aliases_vouch_for_name(
        name=name_b, aliases=aliases_a, conflicting_component=conflicting_component
    ) or _aliases_vouch_for_name(
        name=name_a, aliases=aliases_b, conflicting_component=conflicting_component
    )


def _aliases_vouch_for_name(
    *, name: Name, aliases: Iterable[Name], conflicting_component: NameComponent
) -> bool:
    """Returns whether any of the given aliases names the same person as the
    given primary name, matching loosely on the conflicting component and
    agreeing on the rest."""
    return any(
        _conflicting_component_names_match_loosely(
            alias.value_for(conflicting_component),
            name.value_for(conflicting_component),
        )
        and _names_otherwise_agree(alias, name, ignoring=conflicting_component)
        for alias in aliases
    )


def _conflicting_component_names_match_loosely(
    component_a: str | None, component_b: str | None
) -> bool:
    return names_match_loosely(component_a, component_b)


def _names_otherwise_agree(alias: Name, name: Name, *, ignoring: NameComponent) -> bool:
    """Returns whether an alias and a primary name agree on every component
    both of them record, other than the one whose conflict is being excused.
    Two recorded values agree when they loosely match or one is a lone initial
    matching the other's first letter, so the alias "KIMBERLY L JOHNSON"
    agrees with the primary "KIMBERLY LEE JOHNSON". Name suffixes agree unless
    both canonicalize to different generations, so an alias of "JOHN SMITH SR"
    cannot vouch for a primary "JOHN SMITH JR". A value missing on either side
    never disagrees, so a sparse alias row is not penalized for what it
    omits."""
    for other in NameComponent:
        if other is ignoring:
            continue
        normalized_a = normalize_name(alias.value_for(other))
        normalized_b = normalize_name(name.value_for(other))
        if not normalized_a or not normalized_b:
            continue
        if names_match_loosely(normalized_a, normalized_b):
            continue
        if is_initial_match(normalized_a, normalized_b):
            continue
        return False
    return not are_name_suffixes_in_conflict(alias.name_suffix, name.name_suffix)
