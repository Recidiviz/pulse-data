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
"""IdentityCluster data class for the identity ingest pipeline."""

from __future__ import annotations

import attr

from recidiviz.common.attr_validators import is_list_of, is_str
from recidiviz.persistence.entity.batch_identity_clustering.entities import (
    IdentityAttributes,
    IdentityExternalId,
)


@attr.s(eq=False, kw_only=True)
class IdentityCluster:
    """Result of clustering identity fragments for one logical person."""

    cluster_id: str = attr.ib(validator=is_str)
    external_ids: list[IdentityExternalId] = attr.ib(
        validator=is_list_of(IdentityExternalId)
    )
    chosen_attributes: IdentityAttributes = attr.ib(
        validator=attr.validators.instance_of(IdentityAttributes)
    )
    # SHA-256 of the cluster's sorted external IDs and normalized attribute
    # values, used by the Identity Service to detect whether a cluster has
    # changed since the last import.
    cluster_hash: str = attr.ib(validator=is_str)
