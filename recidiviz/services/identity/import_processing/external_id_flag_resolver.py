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
"""Resolver that sets the display and stable id flags on an identity's
external id rows during import."""
from collections import defaultdict

from recidiviz.common.constants.identity import IdentifierType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.external_id_selection import (
    select_display_external_id,
    select_stable_external_id,
)
from recidiviz.persistence.database.schema.identity import schema


def resolve_display_and_stable_id_flags(
    *, tenant: Tenant, external_id_rows: list[schema.ExternalId]
) -> None:
    """Sets is_current_display_id_for_type and is_stable_id_for_type on the
    given rows, which must be one identity's complete set of external id rows:
    the flags mark a choice among all of the identity's active ids of a type,
    so resolving over a partial set could flag the wrong row or flag two.

    For each id_type, exactly one active row gets each flag. The only active
    row of its type trivially gets both, with no selection strategy consulted,
    which is what lets a tenant with no registered strategies (e.g. a non-state
    tenant) carry single ids of any type. When a type has several active rows,
    the shared selection module picks the display and stable id for the tenant
    and id type, and raises for a (tenant, id_type) with no registered
    strategy, so an unimplemented multi-id tenant fails loudly rather than
    displaying an arbitrary id. Inactive rows never carry either flag.
    """
    active_rows_by_id_type: dict[IdentifierType, list[schema.ExternalId]] = defaultdict(
        list
    )
    for row in external_id_rows:
        row.is_current_display_id_for_type = False
        row.is_stable_id_for_type = False
        if row.is_active:
            active_rows_by_id_type[row.id_type].append(row)
    for id_type, rows in active_rows_by_id_type.items():
        if len(rows) == 1:
            rows[0].is_current_display_id_for_type = True
            rows[0].is_stable_id_for_type = True
            continue
        external_ids = [row.external_id for row in rows]
        display_id = select_display_external_id(
            tenant=tenant, id_type=id_type.value, external_ids=external_ids
        )
        stable_id = select_stable_external_id(
            tenant=tenant, id_type=id_type.value, external_ids=external_ids
        )
        for row in rows:
            row.is_current_display_id_for_type = row.external_id == display_id
            row.is_stable_id_for_type = row.external_id == stable_id
