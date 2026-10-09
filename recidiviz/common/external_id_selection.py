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
"""Shared selection of the display and stable external id for a given tenant
and id type.

When an entity has more than one external id of a single id_type, one of them
is chosen to be the display id for that type (the id surfaced when a product
shows an external id of this type) and another is chosen to be the stable id
for that type (the id kept constant as the entity accrues new ids of the type,
so products can persist it and look the entity up later). This module makes
both choices for every (Tenant, id_type) whose selection is a pure function
of the external id strings themselves.

TODO(OBT-51936): Currently this module works from external id strings alone,
because that is all an Identity Service cluster currently carries. A
(Tenant, id_type) whose selection needs id-active dates (recency) or ingest-set
flags is not handled here; the activity pipeline selects those itself. Once the
cluster carries id-active dates, grow this module to make the recency-based
selections too, so those (Tenant, id_type) pairs no longer need the activity
pipeline to pick them.
"""
from collections import defaultdict
from enum import Enum
from typing import Callable

from recidiviz.common.constants.state.external_id_types import (
    US_AR_PARTYID,
    US_AZ_PERSON_ID,
    US_CA_BADGE_NO,
    US_CO_PARTYID,
    US_IX_CIS_EMPL_CD,
    US_IX_EMPLOYEE,
    US_IX_STAFF_ID,
    US_ME_EMPLOYEE,
    US_MI_COMPAS_USER,
    US_MI_DOC_BOOK,
    US_MI_OMNI_USER,
    US_ND_DOCSTARS_OFFICER,
    US_ND_ELITE,
    US_ND_ELITE_BOOKING,
    US_ND_SID,
    US_NE_PAROLE_STAFF_ID,
    US_PA_CONT,
    US_PA_INMATE,
    US_PA_PBPP,
    US_PA_PBPP_POSNO,
    US_TX_EMAIL,
    US_TX_STAFF_ID,
    US_TX_TDCJ,
)
from recidiviz.common.constants.tenants import Tenant

# Width to zero-pad numeric-string external ids to before comparing them, so
# that they order numerically (e.g. "1237" after "429") rather than
# lexicographically.
_NUMERIC_LPAD_WIDTH = 15


class ExternalIdSelectionStrategy(Enum):
    """How to order the external ids of a type when choosing the display and
    stable id.

    Under every strategy the display id is the highest-ordered external id and
    the stable id is the lowest-ordered one.
    """

    # Order lexicographically by the external id string.
    ALPHABETICAL = "ALPHABETICAL"

    # Order numerically, by the external id string zero-padded to a fixed width.
    NUMERIC_LPAD = "NUMERIC_LPAD"


# The selection strategy for each (tenant, id_type) whose display/stable choice
# is a pure function of the external id strings, ordered by tenant. A (tenant,
# id_type) absent here has no shared selection strategy, so select_* raises for
# it. TODO(OBT-51936): Tenants whose selection depends on id-active dates or
# ingest-set flags (US_CA person, US_NE person, US_NYC person) are deliberately
# absent and handled in the activity pipeline instead; move them here once the
# cluster carries id-active dates.
#
# TODO(OBT-53872): Fold this per-tenant registry into each tenant's
# identity_config.yaml, alongside the allowed-multiples declarations, leaving
# only the generic strategies in common.
_SELECTION_STRATEGY_BY_TENANT_AND_ID_TYPE: dict[
    tuple[Tenant, str], ExternalIdSelectionStrategy
] = {
    (Tenant.US_AR, US_AR_PARTYID): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_AZ, US_AZ_PERSON_ID): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_CA, US_CA_BADGE_NO): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_CO, US_CO_PARTYID): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_IX, US_IX_CIS_EMPL_CD): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_IX, US_IX_EMPLOYEE): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_IX, US_IX_STAFF_ID): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_ME, US_ME_EMPLOYEE): ExternalIdSelectionStrategy.ALPHABETICAL,
    # US_MI OMNI and COMPAS staff ids are numeric strings compared numerically.
    (Tenant.US_MI, US_MI_COMPAS_USER): ExternalIdSelectionStrategy.NUMERIC_LPAD,
    (Tenant.US_MI, US_MI_OMNI_USER): ExternalIdSelectionStrategy.NUMERIC_LPAD,
    (Tenant.US_MI, US_MI_DOC_BOOK): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_ND, US_ND_DOCSTARS_OFFICER): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_ND, US_ND_ELITE): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_ND, US_ND_ELITE_BOOKING): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_ND, US_ND_SID): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_NE, US_NE_PAROLE_STAFF_ID): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_PA, US_PA_CONT): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_PA, US_PA_INMATE): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_PA, US_PA_PBPP): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_PA, US_PA_PBPP_POSNO): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_TX, US_TX_EMAIL): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_TX, US_TX_STAFF_ID): ExternalIdSelectionStrategy.ALPHABETICAL,
    (Tenant.US_TX, US_TX_TDCJ): ExternalIdSelectionStrategy.ALPHABETICAL,
}


def shared_selection_tenant_and_id_type_keys() -> set[tuple[Tenant, str]]:
    """Returns the (tenant, id_type) pairs this module selects display/stable
    ids for."""
    return set(_SELECTION_STRATEGY_BY_TENANT_AND_ID_TYPE)


def has_shared_selection_strategy(*, tenant: Tenant, id_type: str) -> bool:
    """Returns whether this module owns the display/stable selection for the
    given tenant and id type."""
    return (tenant, id_type) in _SELECTION_STRATEGY_BY_TENANT_AND_ID_TYPE


def select_display_external_id(
    *, tenant: Tenant, id_type: str, external_ids: list[str]
) -> str:
    """Returns the external id to use as the display id for the given tenant and
    id type.

    Raises if (tenant, id_type) has no shared selection strategy, or if
    external_ids is empty or holds two ids the selection strategy considers
    identical.
    """
    sort_key = _sort_key(tenant=tenant, id_type=id_type)
    _validate_candidates(
        tenant=tenant, id_type=id_type, external_ids=external_ids, sort_key=sort_key
    )
    return max(external_ids, key=sort_key)


def select_stable_external_id(
    *, tenant: Tenant, id_type: str, external_ids: list[str]
) -> str:
    """Returns the external id to use as the stable id for the given tenant and
    id type.

    Raises if (tenant, id_type) has no shared selection strategy, or if
    external_ids is empty or holds two ids the selection strategy considers
    identical.
    """
    sort_key = _sort_key(tenant=tenant, id_type=id_type)
    _validate_candidates(
        tenant=tenant, id_type=id_type, external_ids=external_ids, sort_key=sort_key
    )
    return min(external_ids, key=sort_key)


def _sort_key(*, tenant: Tenant, id_type: str) -> Callable[[str], str]:
    """Returns the ordering key for the (tenant, id_type)'s selection strategy."""
    strategy = _SELECTION_STRATEGY_BY_TENANT_AND_ID_TYPE.get((tenant, id_type))
    if strategy is None:
        # TODO(OBT-51936): Drop the activity-pipeline fallback from this message
        # once the cluster carries id-active dates and this module makes the
        # recency-based selections too.
        raise ValueError(
            f"No shared display/stable selection strategy for tenant "
            f"[{tenant.value}] and id_type [{id_type}]. First confirm this id_type "
            f"legitimately allows multiple ids per entity (see the DO NOT ADD warning in "
            f"multiple_external_id_helpers.py). If only a handful of entities have "
            f"duplicates, it is likely a data error to fix via raw data migrations. If it "
            f"is a real allowed-multiple, add a strategy here, or handle it in the "
            f"activity pipeline if its selection depends on id-active dates or ingest-set "
            f"flags."
        )
    if strategy is ExternalIdSelectionStrategy.ALPHABETICAL:
        return lambda external_id: external_id
    if strategy is ExternalIdSelectionStrategy.NUMERIC_LPAD:
        return lambda external_id: external_id.zfill(_NUMERIC_LPAD_WIDTH)
    raise ValueError(f"Unhandled selection strategy [{strategy}]")


def _validate_candidates(
    *,
    tenant: Tenant,
    id_type: str,
    external_ids: list[str],
    sort_key: Callable[[str], str],
) -> None:
    """Raises if external_ids is empty or holds two ids the selection strategy
    considers identical: literal duplicates under any strategy, and additionally
    ids differing only in zero-padding (e.g. 0123 vs 123) under NUMERIC_LPAD."""
    if not external_ids:
        raise ValueError(
            f"Cannot select a display/stable external id for tenant "
            f"[{tenant.value}] and id_type [{id_type}] from an empty list."
        )
    ids_by_key: dict[str, list[str]] = defaultdict(list)
    for external_id in external_ids:
        ids_by_key[sort_key(external_id)].append(external_id)
    identical_ids = sorted(ids for ids in ids_by_key.values() if len(ids) > 1)
    if identical_ids:
        raise ValueError(
            f"Found external ids {identical_ids} of id_type [{id_type}] for tenant "
            f"[{tenant.value}] that the selection strategy considers identical, so "
            f"neither can be picked deterministically. Literal duplicates should have "
            f"been merged before selecting a display/stable id; numeric ids differing "
            f"only in zero-padding are likely the same id recorded inconsistently, to "
            f"fix upstream."
        )
