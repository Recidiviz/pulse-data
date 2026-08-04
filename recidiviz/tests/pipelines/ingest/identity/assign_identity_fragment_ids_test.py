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
"""Tests for assign_identity_fragment_ids.py"""
import copy
import unittest

from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.identity import identity_fragment_entities
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityName,
)
from recidiviz.persistence.entity.serialization import serialize_entity_into_json
from recidiviz.pipelines.ingest.identity.assign_identity_fragment_ids import (
    assign_identity_fragment_id,
)
from recidiviz.tests.persistence.entity.identity.entities_test_utils import (
    generate_full_graph_identity_fragment,
)
from recidiviz.utils.types import assert_type


class TestAssignIdentityFragmentId(unittest.TestCase):
    """Tests for assign_identity_fragment_id."""

    def test_assigns_nonnull_id(self) -> None:
        fragment = generate_full_graph_identity_fragment(set_back_edges=False)
        self.assertIsNone(fragment.identity_fragment_id)

        result = assign_identity_fragment_id(fragment)
        self.assertIsNotNone(result.identity_fragment_id)
        self.assertTrue(result.identity_fragment_id)

    def test_does_not_mutate_input(self) -> None:
        fragment = generate_full_graph_identity_fragment(set_back_edges=False)
        attributes = assert_type(fragment.attributes, IdentityAttributes)
        # Put a child list into non-sorted order. Serializing a fragment sorts
        # its child lists in place, so if the hashing step ran against the input
        # rather than a copy, this order would change.
        attributes.races.reverse()
        races_before = list(attributes.races)

        _ = assign_identity_fragment_id(fragment)

        # The input fragment (which also feeds the clustering branch) is left
        # untouched: no id assigned, no back edges wired, child order preserved.
        self.assertIsNone(fragment.identity_fragment_id)
        self.assertIsNone(attributes.fragment)
        self.assertEqual(races_before, attributes.races)

    def test_id_is_deterministic_for_equal_content(self) -> None:
        first = assign_identity_fragment_id(
            generate_full_graph_identity_fragment(set_back_edges=False)
        )
        second = assign_identity_fragment_id(
            generate_full_graph_identity_fragment(set_back_edges=False)
        )
        self.assertEqual(first.identity_fragment_id, second.identity_fragment_id)

    def test_id_differs_for_different_content(self) -> None:
        fragment = generate_full_graph_identity_fragment(set_back_edges=False)
        other = copy.deepcopy(fragment)
        other_name = assert_type(
            assert_type(other.attributes, IdentityAttributes).name, IdentityName
        )
        other_name.given_name = "A Different Given Name"

        self.assertNotEqual(
            assign_identity_fragment_id(fragment).identity_fragment_id,
            assign_identity_fragment_id(other).identity_fragment_id,
        )

    def test_child_rows_carry_fragment_id_fk(self) -> None:
        """After assignment, serializing a child entity yields a row whose
        identity_fragment_id FK matches the root's id, proving back edges were
        wired through the intermediate IdentityAttributes."""
        result = assign_identity_fragment_id(
            generate_full_graph_identity_fragment(set_back_edges=False)
        )
        name = assert_type(
            assert_type(result.attributes, IdentityAttributes).name, IdentityName
        )

        name_row = serialize_entity_into_json(
            name, entities_module_context_for_module(identity_fragment_entities)
        )
        self.assertEqual(result.identity_fragment_id, name_row["identity_fragment_id"])
