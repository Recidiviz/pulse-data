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
"""Assigns each IdentityFragment a deterministic content-hash identity_fragment_id
that identifies the fragment across the pipeline's branches, including as the join
key for the debug {tenant}_identity_fragment.* output tables.
"""
import copy

from recidiviz.common.common_utils import get_hash_of_json
from recidiviz.persistence.entity.entity_utils import (
    set_backedges_allowing_intermediate_entities,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities_module_context import (
    IDENTITY_FRAGMENT_ENTITIES_CONTEXT,
)
from recidiviz.persistence.entity.serialization import serialize_entity_tree_into_json

IDENTITY_FRAGMENT_ID_FIELD = "identity_fragment_id"


def assign_identity_fragment_id(fragment: IdentityFragment) -> IdentityFragment:
    """Returns a copy of the fragment with its identity_fragment_id set to a
    deterministic SHA-256 of the fragment's content."""

    # Deep copy so the assignment never mutates a Beam element in place.
    fragment = copy.deepcopy(fragment)

    json_entity_tree = serialize_entity_tree_into_json(
        fragment, IDENTITY_FRAGMENT_ENTITIES_CONTEXT
    )
    json_entity_tree.pop(IDENTITY_FRAGMENT_ID_FIELD)
    identity_fragment_id = get_hash_of_json(json_entity_tree)

    # Set back edges so each child row carries an identity_fragment_id
    # when the tree is serialized to rows.
    set_backedges_allowing_intermediate_entities(
        fragment, IDENTITY_FRAGMENT_ENTITIES_CONTEXT
    )
    fragment.identity_fragment_id = identity_fragment_id

    if not fragment.identity_fragment_id:
        raise ValueError(
            f"Failed to assign an identity_fragment_id to fragment [{fragment}]."
        )
    return fragment
