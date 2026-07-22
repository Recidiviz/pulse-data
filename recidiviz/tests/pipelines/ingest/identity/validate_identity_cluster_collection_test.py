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
"""Tests for validate_identity_cluster_collection."""
import unittest

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
    IdentityClusterName,
)
from recidiviz.pipelines.ingest.identity.validate_identity_cluster_collection import (
    ValidateIdentityClusterCollection,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TENANT = Tenant.US_XX


def _cluster(eids: list[tuple[str, str]], tenant: Tenant = _TENANT) -> IdentityCluster:
    return IdentityCluster(
        tenant=tenant,
        external_ids=tuple(
            IdentityClusterExternalId(tenant=tenant, external_id=eid, id_type=id_type)
            for eid, id_type in eids
        ),
        person_type=PersonType.JII,
        name=IdentityClusterName(tenant=tenant, given_name="John", surname="Doe"),
    )


class TestValidateIdentityClusterCollection(unittest.TestCase):
    """Beam pipeline tests for ValidateIdentityClusterCollection's cross-cluster checks."""

    def setUp(self) -> None:
        super().setUp()
        self.test_pipeline = create_test_pipeline()

    def test_valid_clusters_pass_through(self) -> None:
        cluster_a = _cluster([("A", "T1")])
        cluster_b = _cluster([("B", "T2")])

        output = (
            self.test_pipeline
            | beam.Create([cluster_a, cluster_b])
            | ValidateIdentityClusterCollection()
        )

        cluster_ids = output | beam.Map(lambda cluster: cluster.identity_cluster_id)
        assert_that(
            cluster_ids,
            equal_to([cluster_a.identity_cluster_id, cluster_b.identity_cluster_id]),
        )
        self.test_pipeline.run()

    def test_external_id_in_multiple_clusters_fails_pipeline(self) -> None:
        """Two clusters that share an external ID must fail the cross-cluster
        uniqueness check."""
        cluster_a = _cluster([("A", "T1"), ("B", "T2")])
        cluster_b = _cluster([("A", "T1")])

        _ = (
            self.test_pipeline
            | beam.Create([cluster_a, cluster_b])
            | ValidateIdentityClusterCollection()
        )

        with self.assertRaisesRegex(
            Exception,
            r"Found external ids that appear in more than one IdentityCluster:\n"
            r"  \* External id \[\('A', 'T1'\)\] appears in \[2\] clusters: "
            r"\[[0-9a-f]+, [0-9a-f]+\]\.",
        ):
            self.test_pipeline.run()

    def test_no_clusters_fails_pipeline(self) -> None:
        cluster = _cluster([("A", "T1")])

        _ = (
            self.test_pipeline
            | beam.Create([cluster])
            | "Drop all elements" >> beam.Filter(lambda _element: False)
            | ValidateIdentityClusterCollection()
        )

        with self.assertRaisesRegex(
            Exception,
            r"Expected the pipeline to produce at least one IdentityCluster, "
            r"but none were produced\.",
        ):
            self.test_pipeline.run()
