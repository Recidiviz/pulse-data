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
"""Tests for identity_cluster_entities_utils."""
import unittest

from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
    IdentityClusterName,
    IdentityClusterRace,
)
from recidiviz.persistence.entity.identity.identity_cluster_entities_utils import (
    cluster_entity_class_with_fragment_class_name,
    get_fragment_entity_class_for_cluster_entity,
)
from recidiviz.persistence.entity.identity.identity_cluster_entity import (
    IdentityClusterEntity,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityExternalId,
    IdentityName,
    IdentityRace,
)


class TestIdentityClusterEntitiesUtils(unittest.TestCase):
    """Tests for the fragment <-> cluster entity-class mapping helpers."""

    def test_fragment_class_name_strips_cluster_infix(self) -> None:
        self.assertEqual(IdentityClusterName.fragment_class_name(), "IdentityName")
        self.assertEqual(IdentityClusterRace.fragment_class_name(), "IdentityRace")
        self.assertEqual(
            IdentityClusterExternalId.fragment_class_name(), "IdentityExternalId"
        )

    def test_cluster_entity_class_with_fragment_class_name(self) -> None:
        self.assertIs(
            cluster_entity_class_with_fragment_class_name(IdentityName.__name__),
            IdentityClusterName,
        )
        self.assertIs(
            cluster_entity_class_with_fragment_class_name(IdentityRace.__name__),
            IdentityClusterRace,
        )
        self.assertIs(
            cluster_entity_class_with_fragment_class_name(IdentityExternalId.__name__),
            IdentityClusterExternalId,
        )

    def test_identity_cluster_root_fragment_class_name_raises(self) -> None:
        """The cluster root has no single fragment counterpart, so calling
        `fragment_class_name()` on it raises (rather than silently returning
        the bogus `Identity` name the base implementation would produce)."""
        with self.assertRaisesRegex(
            NotImplementedError,
            r"\[IdentityCluster\] is the flattened root of the cluster tree "
            r"and has no single fragment-tree counterpart",
        ):
            IdentityCluster.fragment_class_name()

    def test_cluster_entity_class_with_root_fragment_class_name_raises(self) -> None:
        """Looking up by the bogus fragment name the base implementation would
        have produced for the root (`Identity`) raises ValueError instead of
        phantom-matching `IdentityCluster`."""
        with self.assertRaisesRegex(
            ValueError,
            r"No IdentityClusterEntity class corresponding with a fragment class "
            r"name of: \[Identity\]",
        ):
            cluster_entity_class_with_fragment_class_name("Identity")

    def test_cluster_entity_class_with_unknown_fragment_class_name_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"No IdentityClusterEntity class corresponding with a fragment class "
            r"name of: \[IdentityNonexistent\]",
        ):
            cluster_entity_class_with_fragment_class_name("IdentityNonexistent")

    def test_get_fragment_entity_class_for_cluster_entity(self) -> None:
        self.assertIs(
            get_fragment_entity_class_for_cluster_entity(IdentityClusterName),
            IdentityName,
        )
        self.assertIs(
            get_fragment_entity_class_for_cluster_entity(IdentityClusterExternalId),
            IdentityExternalId,
        )

    def test_every_cluster_child_entity_maps_to_a_fragment_class(self) -> None:
        """Every non-root cluster entity has a fragment-tree counterpart that
        round-trips back to it."""
        fragment_class_names = {
            cls.__name__
            for cls in get_all_entity_classes_in_module(identity_fragment_entities)
        }
        cluster_child_classes = [
            cls
            for cls in get_all_entity_classes_in_module(identity_cluster_entities)
            if cls is not IdentityCluster
        ]
        # Guard against the discovery silently returning nothing.
        self.assertGreater(len(cluster_child_classes), 1)
        for cluster_cls in cluster_child_classes:
            with self.subTest(cluster_cls=cluster_cls.__name__):
                assert issubclass(cluster_cls, IdentityClusterEntity)
                fragment_name = cluster_cls.fragment_class_name()
                self.assertIn(fragment_name, fragment_class_names)
                self.assertIs(
                    cluster_entity_class_with_fragment_class_name(fragment_name),
                    cluster_cls,
                )
