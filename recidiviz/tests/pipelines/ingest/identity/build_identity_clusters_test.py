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
"""Tests for build_identity_clusters."""
import unittest
from collections.abc import Iterable

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.common.constants.identity import PersonType
from recidiviz.persistence.entity.identity.entities import (
    IdentityAttributes,
    IdentityExternalId,
    IdentityFragment,
    IdentityName,
)
from recidiviz.pipelines.ingest.identity.build_identity_clusters import (
    CLUSTER_MEMBERSHIPS,
    FRAGMENTS_WITH_DATES,
    BuildIdentityClusters,
)
from recidiviz.pipelines.ingest.identity.identity_cluster import IdentityCluster
from recidiviz.pipelines.ingest.types import ExternalIdKey
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TENANT = "US_XX"


def _eid_entity(external_id: str, id_type: str) -> IdentityExternalId:
    return IdentityExternalId(tenant=_TENANT, external_id=external_id, id_type=id_type)


def _fragment(
    eids: list[tuple[str, str]],
    name_given: str | None = None,
    name_surname: str | None = None,
    person_type: PersonType = PersonType.JII,
) -> IdentityFragment:
    name = None
    if name_given or name_surname:
        name = IdentityName(tenant=_TENANT, given_name=name_given, surname=name_surname)
    return IdentityFragment(
        tenant=_TENANT,
        external_ids=[_eid_entity(eid, id_type) for eid, id_type in eids],
        attributes=IdentityAttributes(
            tenant=_TENANT, person_type=person_type, name=name
        ),
    )


def _cluster_summary(
    cluster: IdentityCluster,
) -> tuple[str, tuple[tuple[str, str], ...], str | None, str]:
    """Extract comparable fields from an IdentityCluster."""
    eid_tuple = tuple(sorted((e.external_id, e.id_type) for e in cluster.external_ids))
    given_name = cluster.attributes.name.given_name if cluster.attributes.name else None
    return (cluster.cluster_id, eid_tuple, given_name, cluster.cluster_hash)


class TestRekeyFragmentsByCluster(unittest.TestCase):
    """Tests for rekey_fragments_by_cluster."""

    def setUp(self) -> None:
        super().setUp()
        self.transform = BuildIdentityClusters(tenant=_TENANT)

    def test_eid_with_cluster_and_fragments(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        cluster_key = (("A", "T1"), ("B", "T2"))
        fragment = _fragment([("A", "T1")])
        element: tuple[ExternalIdKey, dict[str, Iterable]] = (
            eid_key,
            {
                CLUSTER_MEMBERSHIPS: [cluster_key],
                FRAGMENTS_WITH_DATES: [(100.0, "view_a", fragment)],
            },
        )

        emitted = list(self.transform.rekey_fragments_by_cluster(element))

        self.assertEqual(len(emitted), 1)
        (emitted_key, fragment_with_date) = emitted[0]
        self.assertEqual(emitted_key, cluster_key)
        self.assertEqual(fragment_with_date, (100.0, "view_a", fragment))

    def test_eid_with_multiple_fragments_yields_one_per_fragment(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        cluster_key = (("A", "T1"),)
        fragment_a = _fragment([("A", "T1")], name_given="John")
        fragment_b = _fragment([("A", "T1")], name_given="Jonathan")
        element: tuple[ExternalIdKey, dict[str, Iterable]] = (
            eid_key,
            {
                CLUSTER_MEMBERSHIPS: [cluster_key],
                FRAGMENTS_WITH_DATES: [
                    (100.0, "view_a", fragment_a),
                    (200.0, "view_a", fragment_b),
                ],
            },
        )

        emitted = list(self.transform.rekey_fragments_by_cluster(element))

        self.assertEqual(len(emitted), 2)
        self.assertEqual([k for k, _ in emitted], [cluster_key, cluster_key])

    def test_eid_with_no_cluster_raises(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1")])
        element: tuple[ExternalIdKey, dict[str, Iterable]] = (
            eid_key,
            {
                CLUSTER_MEMBERSHIPS: [],
                FRAGMENTS_WITH_DATES: [(100.0, "view_a", fragment)],
            },
        )

        with self.assertRaisesRegex(
            ValueError,
            r"External id \('A', 'T1'\) is expected to be in one cluster "
            r"but is in 0\.",
        ):
            list(self.transform.rekey_fragments_by_cluster(element))

    def test_eid_in_multiple_clusters_raises(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        cluster_key_1 = (("A", "T1"), ("B", "T2"))
        cluster_key_2 = (("A", "T1"), ("C", "T3"))
        fragment = _fragment([("A", "T1")])
        element: tuple[ExternalIdKey, dict[str, Iterable]] = (
            eid_key,
            {
                CLUSTER_MEMBERSHIPS: [cluster_key_1, cluster_key_2],
                FRAGMENTS_WITH_DATES: [(100.0, "view_a", fragment)],
            },
        )

        with self.assertRaisesRegex(
            ValueError,
            r"External id \('A', 'T1'\) is expected to be in one cluster "
            r"but is in 2\.",
        ):
            list(self.transform.rekey_fragments_by_cluster(element))

    def test_eid_in_cluster_no_fragments_yields_nothing(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        cluster_key = (("A", "T1"),)
        element: tuple[ExternalIdKey, dict[str, Iterable]] = (
            eid_key,
            {
                CLUSTER_MEMBERSHIPS: [cluster_key],
                FRAGMENTS_WITH_DATES: [],
            },
        )

        emitted = list(self.transform.rekey_fragments_by_cluster(element))

        self.assertEqual(emitted, [])


class TestBuildCluster(unittest.TestCase):
    """Tests for build_cluster."""

    def setUp(self) -> None:
        super().setUp()
        self.transform = BuildIdentityClusters(tenant=_TENANT)

    def test_single_eid_single_fragment(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        cluster_key = (eid_key,)
        element = (cluster_key, [(100.0, "view_a", fragment)])

        result = self.transform.build_cluster(element)

        self.assertEqual(len(result.external_ids), 1)
        self.assertEqual(result.external_ids[0].external_id, "A")
        assert result.attributes.name is not None
        self.assertEqual(result.attributes.name.given_name, "John")

    def test_multi_eid_merges_attributes(self) -> None:
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T2")
        fragment_a = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        fragment_b = _fragment([("B", "T2")])
        cluster_key = tuple(sorted([eid_a, eid_b]))
        element = (
            cluster_key,
            [
                (100.0, "view_a", fragment_a),
                (200.0, "view_a", fragment_b),
            ],
        )

        result = self.transform.build_cluster(element)

        self.assertEqual(len(result.external_ids), 2)
        assert result.attributes.name is not None
        self.assertEqual(result.attributes.name.given_name, "John")

    def test_eid_with_no_fragments_still_in_external_ids(self) -> None:
        """An EID that's in the cluster but has no fragments still appears in
        the resulting cluster's external_ids, because external_ids are derived
        from the cluster_key (the upstream-determined cluster membership), not
        from the fragments themselves."""
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T2")
        fragment = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        cluster_key = tuple(sorted([eid_a, eid_b]))
        element = (cluster_key, [(100.0, "view_a", fragment)])

        result = self.transform.build_cluster(element)

        eid_pairs = {(e.external_id, e.id_type) for e in result.external_ids}
        self.assertEqual(eid_pairs, {("A", "T1"), ("B", "T2")})
        assert result.attributes.name is not None
        self.assertEqual(result.attributes.name.given_name, "John")

    def test_no_fragments_raises(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        cluster_key = (eid_key,)
        fragments: list[tuple[float, str, IdentityFragment]] = []
        element = (cluster_key, fragments)

        with self.assertRaisesRegex(ValueError, "has no fragments"):
            self.transform.build_cluster(element)

    def test_no_attributes_beyond_person_type_raises(self) -> None:
        """A fragment that contributes only external IDs (all attribute fields
        None/empty) should cause the cluster build to fail."""
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1")])
        cluster_key = (eid_key,)
        element = (cluster_key, [(100.0, "view_a", fragment)])

        with self.assertRaisesRegex(ValueError, "has no attributes"):
            self.transform.build_cluster(element)

    def test_single_eid_conflicting_person_types_raises(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        jii_fragment = _fragment([("A", "T1")], person_type=PersonType.JII)
        staff_fragment = _fragment([("A", "T1")], person_type=PersonType.STAFF)
        cluster_key = (eid_key,)
        element = (
            cluster_key,
            [
                (100.0, "view_a", jii_fragment),
                (200.0, "view_b", staff_fragment),
            ],
        )

        with self.assertRaisesRegex(
            ValueError,
            "Conflicting non-None values for 'attributes.person_type'",
        ):
            self.transform.build_cluster(element)

    def test_cross_eid_conflicting_person_types_raises(self) -> None:
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T2")

        jii_fragment = _fragment([("A", "T1")], person_type=PersonType.JII)
        staff_fragment = _fragment([("B", "T2")], person_type=PersonType.STAFF)

        cluster_key = tuple(sorted([eid_a, eid_b]))

        element = (
            cluster_key,
            [
                (100.0, "view_a", jii_fragment),
                (200.0, "view_b", staff_fragment),
            ],
        )

        with self.assertRaisesRegex(
            ValueError,
            "Conflicting non-None values for 'attributes.person_type'",
        ):
            self.transform.build_cluster(element)

    def test_cluster_id_and_hash_are_set(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        cluster_key = (eid_key,)
        element = (cluster_key, [(100.0, "view_a", fragment)])

        result = self.transform.build_cluster(element)

        expected = IdentityCluster(
            external_ids=[_eid_entity("A", "T1")],
            attributes=result.attributes,
        )
        self.assertEqual(result.cluster_id, expected.cluster_id)
        self.assertEqual(result.cluster_hash, expected.cluster_hash)


class TestBuildIdentityClustersBeam(unittest.TestCase):
    """Beam pipeline tests for BuildIdentityClusters."""

    def setUp(self) -> None:
        super().setUp()
        self.test_pipeline = create_test_pipeline()

    def test_single_eid_cluster(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        cluster_key = (eid_key,)
        fragment = _fragment([("A", "T1")], name_given="John", name_surname="Doe")

        cluster_memberships = self.test_pipeline | "Create memberships" >> beam.Create(
            [(eid_key, cluster_key)]
        )
        fragments = self.test_pipeline | "Create fragments" >> beam.Create(
            [(eid_key, (100.0, "view_a", fragment))]
        )
        output = {
            CLUSTER_MEMBERSHIPS: cluster_memberships,
            FRAGMENTS_WITH_DATES: fragments,
        } | BuildIdentityClusters(tenant=_TENANT)

        expected_cluster = IdentityCluster(
            external_ids=[_eid_entity("A", "T1")],
            attributes=IdentityAttributes(
                tenant=_TENANT,
                person_type=PersonType.JII,
                name=IdentityName(tenant=_TENANT, given_name="John", surname="Doe"),
            ),
        )

        summaries = output | "Summarize" >> beam.Map(_cluster_summary)
        assert_that(
            summaries,
            equal_to(
                [
                    (
                        expected_cluster.cluster_id,
                        (("A", "T1"),),
                        "John",
                        expected_cluster.cluster_hash,
                    )
                ]
            ),
        )
        self.test_pipeline.run()

    def test_multi_eid_cluster(self) -> None:
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T2")
        cluster_key = tuple(sorted({eid_a, eid_b}))
        fragment_a = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        fragment_b = _fragment([("B", "T2")])

        cluster_memberships = self.test_pipeline | "Create memberships" >> beam.Create(
            [
                (eid_a, cluster_key),
                (eid_b, cluster_key),
            ]
        )
        fragments = self.test_pipeline | "Create fragments" >> beam.Create(
            [
                (eid_a, (100.0, "view_a", fragment_a)),
                (eid_b, (200.0, "view_a", fragment_b)),
            ]
        )
        output = {
            CLUSTER_MEMBERSHIPS: cluster_memberships,
            FRAGMENTS_WITH_DATES: fragments,
        } | BuildIdentityClusters(tenant=_TENANT)

        summaries = output | "Summarize" >> beam.Map(_cluster_summary)

        expected_cluster = IdentityCluster(
            external_ids=[_eid_entity("A", "T1"), _eid_entity("B", "T2")],
            attributes=IdentityAttributes(
                tenant=_TENANT,
                person_type=PersonType.JII,
                name=IdentityName(tenant=_TENANT, given_name="John", surname="Doe"),
            ),
        )

        assert_that(
            summaries,
            equal_to(
                [
                    (
                        expected_cluster.cluster_id,
                        (("A", "T1"), ("B", "T2")),
                        "John",
                        expected_cluster.cluster_hash,
                    )
                ]
            ),
        )
        self.test_pipeline.run()

    def test_fragment_with_no_cluster_raises(self) -> None:
        eid_in_cluster: ExternalIdKey = ("A", "T1")
        eid_orphan: ExternalIdKey = ("Z", "T9")
        cluster_key = (eid_in_cluster,)
        fragment_a = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        fragment_b = _fragment([("Z", "T9")], name_given="Orphan")

        cluster_memberships = self.test_pipeline | "Create memberships" >> beam.Create(
            [(eid_in_cluster, cluster_key)]
        )
        fragments = self.test_pipeline | "Create fragments" >> beam.Create(
            [
                (eid_in_cluster, (100.0, "view_a", fragment_a)),
                (eid_orphan, (100.0, "view_a", fragment_b)),
            ]
        )
        _ = {
            CLUSTER_MEMBERSHIPS: cluster_memberships,
            FRAGMENTS_WITH_DATES: fragments,
        } | BuildIdentityClusters(tenant=_TENANT)

        with self.assertRaisesRegex(
            Exception,
            r"External id \('Z', 'T9'\) is expected to be in one cluster "
            r"but is in 0\.",
        ):
            self.test_pipeline.run()

    def test_two_separate_clusters(self) -> None:
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T1")
        cluster_key_a = (eid_a,)
        cluster_key_b = (eid_b,)
        fragment_a = _fragment([("A", "T1")], name_given="Alice")
        fragment_b = _fragment([("B", "T1")], name_given="Bob")

        cluster_memberships = self.test_pipeline | "Create memberships" >> beam.Create(
            [
                (eid_a, cluster_key_a),
                (eid_b, cluster_key_b),
            ]
        )
        fragments = self.test_pipeline | "Create fragments" >> beam.Create(
            [
                (eid_a, (100.0, "view_a", fragment_a)),
                (eid_b, (100.0, "view_a", fragment_b)),
            ]
        )
        output = {
            CLUSTER_MEMBERSHIPS: cluster_memberships,
            FRAGMENTS_WITH_DATES: fragments,
        } | BuildIdentityClusters(tenant=_TENANT)

        count = output | "Count" >> beam.combiners.Count.Globally()
        assert_that(count, equal_to([2]))
        self.test_pipeline.run()
