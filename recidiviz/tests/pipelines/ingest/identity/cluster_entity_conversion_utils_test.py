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
"""Tests for cluster_entity_conversion_utils."""
import datetime
import unittest

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
)
from recidiviz.pipelines.ingest.identity.cluster_entity_conversion_utils import (
    convert_attributes_to_cluster_kwargs,
)
from recidiviz.tests.persistence.entity.identity.entities_test_utils import (
    generate_full_graph_identity_cluster,
    generate_full_graph_identity_fragment,
)
from recidiviz.utils.types import assert_type

_TENANT = Tenant.US_XX


class TestConvertAttributesToClusterKwargs(unittest.TestCase):
    """Tests for convert_attributes_to_cluster_kwargs."""

    def test_full_graph_round_trip(self) -> None:
        """Building an IdentityCluster from a converted full-graph fragment
        produces an entity equal to the canonical full-graph cluster helper."""
        fragment = generate_full_graph_identity_fragment(set_back_edges=False)
        attributes = assert_type(fragment.attributes, IdentityAttributes)
        kwargs = convert_attributes_to_cluster_kwargs(attributes)

        cluster = IdentityCluster(
            tenant=_TENANT,
            external_ids=tuple(
                IdentityClusterExternalId(
                    tenant=eid.tenant,
                    external_id=eid.external_id,
                    id_type=eid.id_type,
                )
                for eid in fragment.external_ids
            ),
            **kwargs,
        )

        self.assertEqual(cluster, generate_full_graph_identity_cluster())

    def test_minimal_attributes_yields_none_and_empty_collections(self) -> None:
        """An IdentityAttributes with only one optional attribute populated
        (the minimum the post-init invariant allows) produces kwargs whose
        unpopulated child entities are None and whose unpopulated collection
        fields are empty tuples."""
        attributes = IdentityAttributes(
            tenant=_TENANT,
            person_type=PersonType.JII,
            birthdate=datetime.date(1990, 1, 1),
        )

        kwargs = convert_attributes_to_cluster_kwargs(attributes)

        self.assertEqual(
            kwargs,
            {
                "person_type": PersonType.JII,
                "person_type_raw_text": None,
                "birthdate": datetime.date(1990, 1, 1),
                "name": None,
                "gender": None,
                "sex": None,
                "ethnicity": None,
                "races": (),
                "phone_numbers": (),
                "emails": (),
            },
        )

    def test_collections_become_tuples(self) -> None:
        """List-typed fragment-side collections become tuple-typed cluster-
        side collections, in input order."""
        fragment = generate_full_graph_identity_fragment(set_back_edges=False)
        attributes = assert_type(fragment.attributes, IdentityAttributes)
        kwargs = convert_attributes_to_cluster_kwargs(attributes)

        self.assertIsInstance(kwargs["races"], tuple)
        self.assertIsInstance(kwargs["phone_numbers"], tuple)
        self.assertIsInstance(kwargs["emails"], tuple)

        self.assertEqual(len(kwargs["races"]), len(attributes.races))
        self.assertEqual(
            [r.race for r in kwargs["races"]],
            [r.race for r in attributes.races],
        )
