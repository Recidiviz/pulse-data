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

import hashlib
import json
from typing import Any

import attr

from recidiviz.common.attr_validators import is_list_of
from recidiviz.persistence.entity.identity import entities as identity_entities_module
from recidiviz.persistence.entity.identity.entities import (
    IdentityAttributes,
    IdentityExternalId,
)
from recidiviz.persistence.entity.serialization import (
    serialize_entity_tree_into_json,
    serialize_entity_trees_into_json,
)


@attr.s(eq=False, kw_only=True)
class IdentityCluster:
    """Result of clustering identity fragments for one logical person."""

    external_ids: list[IdentityExternalId] = attr.ib(
        validator=is_list_of(IdentityExternalId)
    )

    attributes: IdentityAttributes = attr.ib(
        validator=attr.validators.instance_of(IdentityAttributes)
    )

    cluster_id: str = attr.ib(init=False)

    # SHA-256 of the cluster's sorted external IDs and attribute values, used
    # by the Identity Service to detect whether a cluster has changed since the
    # last import.
    cluster_hash: str = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        tenant = self.attributes.tenant
        mismatched_tenants = sorted(
            {eid.tenant for eid in self.external_ids if eid.tenant != tenant}
        )
        if mismatched_tenants:
            raise ValueError(
                f"All external_ids must share the cluster's tenant {tenant!r}; "
                f"found mismatched tenants: {mismatched_tenants}"
            )
        self.cluster_id = self._compute_id()
        self.cluster_hash = self._compute_hash()

    def _serialized_external_ids(self) -> list[dict[str, Any]]:
        return serialize_entity_trees_into_json(
            self.external_ids, identity_entities_module
        )

    def _compute_id(self) -> str:
        return self._hash_json(self._serialized_external_ids())

    def _compute_hash(self) -> str:
        return self._hash_json(
            {
                "external_ids": self._serialized_external_ids(),
                "attributes": serialize_entity_tree_into_json(
                    self.attributes, identity_entities_module
                ),
            }
        )

    @staticmethod
    def _hash_json(json_to_serialize: list[dict[str, Any]] | dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(json_to_serialize, sort_keys=True).encode("utf-8")
        ).hexdigest()
