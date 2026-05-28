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
"""Tests for the `generate_full_graph_identity_*` helpers in
`entities_test_utils`."""
import unittest

from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entities_from_tree,
    get_all_entity_classes_in_module,
)
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.tests.persistence.entity.identity.entities_test_utils import (
    generate_full_graph_identity_cluster,
    generate_full_graph_identity_fragment,
)


class TestGenerateFullGraphHelpers(unittest.TestCase):
    """Tests that the `generate_full_graph_identity_*` helpers cover every
    entity class in their respective modules."""

    def test_generate_full_graph_identity_fragment_coverage(self) -> None:
        entities_module_context = entities_module_context_for_module(
            identity_fragment_entities
        )
        expected_entity_classes = get_all_entity_classes_in_module(
            identity_fragment_entities
        )

        found_entity_classes = {
            type(e)
            for e in get_all_entities_from_tree(
                generate_full_graph_identity_fragment(set_back_edges=True),
                entities_module_context,
            )
        }

        missing_in_entity_graph = expected_entity_classes - found_entity_classes
        if missing_in_entity_graph:
            raise ValueError(
                f"Found entities defined in identity_fragment_entities.py which "
                f"are not included in generate_full_graph_identity_fragment(): "
                f"{[cls.__name__ for cls in missing_in_entity_graph]}"
            )

    def test_generate_full_graph_identity_cluster_coverage(self) -> None:
        entities_module_context = entities_module_context_for_module(
            identity_cluster_entities
        )
        expected_entity_classes = get_all_entity_classes_in_module(
            identity_cluster_entities
        )

        found_entity_classes = {
            type(e)
            for e in get_all_entities_from_tree(
                generate_full_graph_identity_cluster(set_back_edges=True),
                entities_module_context,
            )
        }

        missing_in_entity_graph = expected_entity_classes - found_entity_classes
        if missing_in_entity_graph:
            raise ValueError(
                f"Found entities defined in identity_cluster_entities.py which "
                f"are not included in generate_full_graph_identity_cluster(): "
                f"{[cls.__name__ for cls in missing_in_entity_graph]}"
            )
