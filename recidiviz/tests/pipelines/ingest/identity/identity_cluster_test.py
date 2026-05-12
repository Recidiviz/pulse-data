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
"""Unit tests for the IdentityCluster data class."""
import pickle
import unittest

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.persistence.entity.batch_identity_clustering.entities import (
    IdentityAttributes,
    IdentityExternalId,
    IdentityName,
)
from recidiviz.pipelines.ingest.identity.identity_cluster import IdentityCluster

_TENANT = "US_OZ"

_EXTERNAL_ID = IdentityExternalId(
    tenant=_TENANT, external_id="EXT_001", id_type="US_OZ_ID_TYPE"
)
_ATTRIBUTES = IdentityAttributes(
    tenant=_TENANT,
    person_type=PersonType.JII,
    name=IdentityName(tenant=_TENANT, given_name="John", surname="Doe"),
)


class TestIdentityCluster(unittest.TestCase):
    """Tests the IdentityCluster data class."""

    def test_construction(self) -> None:
        cluster = IdentityCluster(
            cluster_id="C1",
            external_ids=[_EXTERNAL_ID],
            chosen_attributes=_ATTRIBUTES,
            cluster_hash="abc123",
        )
        self.assertEqual(cluster.cluster_id, "C1")
        self.assertEqual(cluster.external_ids, [_EXTERNAL_ID])
        self.assertEqual(cluster.chosen_attributes, _ATTRIBUTES)
        self.assertEqual(cluster.cluster_hash, "abc123")

    def test_pickle_roundtrip(self) -> None:
        cluster = IdentityCluster(
            cluster_id="C1",
            external_ids=[_EXTERNAL_ID],
            chosen_attributes=_ATTRIBUTES,
            cluster_hash="abc123",
        )
        restored = pickle.loads(pickle.dumps(cluster))
        self.assertEqual(
            attr.asdict(cluster, recurse=False),
            attr.asdict(restored, recurse=False),
        )
