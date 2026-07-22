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
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
    IdentityClusterName,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
    IdentityName,
)
from recidiviz.pipelines.ingest.identity.build_identity_clusters import (
    CLUSTER_MEMBERSHIPS,
    FRAGMENTS_WITH_DATES,
    BuildIdentityClusters,
)
from recidiviz.pipelines.ingest.types import ExternalIdKey
from recidiviz.tests.persistence.entity.identity.entities_test_utils import (
    identity_fragment_for_test,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TENANT = Tenant.US_XX
_VALID_ID_TYPES = frozenset({"T1", "T2"})


def _fragment(
    eids: list[tuple[str, str]],
    name_given: str | None = None,
    name_surname: str | None = None,
    person_type: PersonType = PersonType.JII,
) -> IdentityFragment:
    """If no name is provided, the fragment is constructed as an
    external-id-only carrier (attributes=None)."""
    name = (
        IdentityName(tenant=_TENANT, given_name=name_given, surname=name_surname)
        if name_given or name_surname
        else None
    )
    return identity_fragment_for_test(
        external_ids=eids, tenant=_TENANT, person_type=person_type, name=name
    )


def _cluster_summary(
    cluster: IdentityCluster,
) -> tuple[str, tuple[tuple[str, str], ...], str | None, str]:
    """Extract comparable fields from an IdentityCluster."""
    eid_tuple = tuple(sorted((e.external_id, e.id_type) for e in cluster.external_ids))
    given_name = cluster.name.given_name if cluster.name else None
    return (cluster.identity_cluster_id, eid_tuple, given_name, cluster.cluster_hash)


class TestRekeyFragmentsByCluster(unittest.TestCase):
    """Tests for rekey_fragments_by_cluster."""

    def setUp(self) -> None:
        super().setUp()
        self.transform = BuildIdentityClusters(
            tenant=_TENANT, valid_id_types=_VALID_ID_TYPES
        )

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
        self.transform = BuildIdentityClusters(
            tenant=_TENANT, valid_id_types=_VALID_ID_TYPES
        )

    def test_single_eid_single_fragment(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        cluster_key = (eid_key,)
        element = (cluster_key, [(100.0, "view_a", fragment)])

        result = self.transform.build_cluster(element)

        self.assertEqual(len(result.external_ids), 1)
        self.assertEqual(result.external_ids[0].external_id, "A")
        self.assertEqual(result.person_type, PersonType.JII)
        assert result.name is not None
        self.assertEqual(result.name.given_name, "John")

    def test_person_type_derived_from_agreeing_fragments(self) -> None:
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T2")
        fragment_a = _fragment(
            [("A", "T1")], name_given="Bob", person_type=PersonType.STAFF
        )
        fragment_b = _fragment([("B", "T2")], person_type=PersonType.STAFF)
        cluster_key = tuple(sorted([eid_a, eid_b]))
        element = (
            cluster_key,
            [
                (100.0, "view_a", fragment_a),
                (200.0, "view_a", fragment_b),
            ],
        )

        result = self.transform.build_cluster(element)

        self.assertEqual(result.person_type, PersonType.STAFF)

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
        assert result.name is not None
        self.assertEqual(result.name.given_name, "John")

    def test_phantom_external_id_raises(self) -> None:
        """An external ID in the cluster that no contributing fragment carries
        is a phantom ID and fails the build."""
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T2")
        fragment = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        cluster_key = tuple(sorted([eid_a, eid_b]))
        element = (cluster_key, [(100.0, "view_a", fragment)])

        with self.assertRaisesRegex(
            ValueError,
            r"Found external ids \[\('B', 'T2'\)\] on the cluster that do not "
            r"appear in any contributing fragment\.",
        ):
            self.transform.build_cluster(element)

    def test_leaked_fragment_external_id_raises(self) -> None:
        """A contributing fragment carrying an external ID not in the cluster
        indicates upstream leakage and fails the build."""
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1"), ("Z", "T2")], name_given="John")
        cluster_key = (eid_key,)
        element = (cluster_key, [(100.0, "view_a", fragment)])

        with self.assertRaisesRegex(
            ValueError,
            r"Found contributing fragments with external ids \[\('Z', 'T2'\)\] "
            r"that are not on the cluster\.",
        ):
            self.transform.build_cluster(element)

    def test_invalid_id_type_raises(self) -> None:
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T9")
        fragment_a = _fragment([("A", "T1")], name_given="John")
        fragment_b = _fragment([("B", "T9")])
        cluster_key = tuple(sorted([eid_a, eid_b]))
        element = (
            cluster_key,
            [
                (100.0, "view_a", fragment_a),
                (200.0, "view_a", fragment_b),
            ],
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found external id types \[T9\] that are not produced by any "
            r"launchable identity ingest view for tenant \[US_XX\]\. "
            r"Valid types: \[T1, T2\]\.",
        ):
            self.transform.build_cluster(element)

    def test_multiple_structural_errors_aggregated(self) -> None:
        """A cluster tripping more than one structural check reports all of
        them under a single header."""
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T9")
        fragment = _fragment([("A", "T1")], name_given="John")
        cluster_key = tuple(sorted([eid_a, eid_b]))
        element = (cluster_key, [(100.0, "view_a", fragment)])

        with self.assertRaisesRegex(
            ValueError,
            r"Found errors for cluster with external ids "
            r"\[\('A', 'T1'\), \('B', 'T9'\)\]:\n"
            r"  \* Found external id types \[T9\](.|\n)*"
            r"  \* Found external ids \[\('B', 'T9'\)\] on the cluster",
        ):
            self.transform.build_cluster(element)

    def test_no_fragments_raises(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        cluster_key = (eid_key,)
        fragments: list[tuple[float, str, IdentityFragment]] = []
        element = (cluster_key, fragments)

        with self.assertRaisesRegex(ValueError, "has no fragments"):
            self.transform.build_cluster(element)

    def test_only_external_id_only_fragments_raises(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1")])
        cluster_key = (eid_key,)
        element = (cluster_key, [(100.0, "view_a", fragment)])

        with self.assertRaisesRegex(
            ValueError,
            r"Failed to build cluster .* no fragment has attributes",
        ):
            self.transform.build_cluster(element)

    def test_single_eid_conflicting_person_types_raises(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        jii_fragment = _fragment(
            [("A", "T1")], name_given="John", person_type=PersonType.JII
        )
        staff_fragment = _fragment(
            [("A", "T1")], name_given="John", person_type=PersonType.STAFF
        )
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
            r"has fragments with conflicting person types \[JII, STAFF\]",
        ):
            self.transform.build_cluster(element)

    def test_cross_eid_conflicting_person_types_raises(self) -> None:
        eid_a: ExternalIdKey = ("A", "T1")
        eid_b: ExternalIdKey = ("B", "T2")

        jii_fragment = _fragment(
            [("A", "T1")], name_given="John", person_type=PersonType.JII
        )
        staff_fragment = _fragment(
            [("B", "T2")], name_given="John", person_type=PersonType.STAFF
        )

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
            r"has fragments with conflicting person types \[JII, STAFF\]",
        ):
            self.transform.build_cluster(element)

    def test_cluster_id_and_hash_are_set(self) -> None:
        eid_key: ExternalIdKey = ("A", "T1")
        fragment = _fragment([("A", "T1")], name_given="John", name_surname="Doe")
        cluster_key = (eid_key,)
        element = (cluster_key, [(100.0, "view_a", fragment)])

        result = self.transform.build_cluster(element)

        self.assertTrue(result.identity_cluster_id)
        self.assertTrue(result.cluster_hash)
        # Re-running on identical input must produce identical hashes (the
        # Identity Service relies on this for deduplication and change
        # detection across import runs).
        rerun = self.transform.build_cluster(element)
        self.assertEqual(result.identity_cluster_id, rerun.identity_cluster_id)
        self.assertEqual(result.cluster_hash, rerun.cluster_hash)


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
        } | BuildIdentityClusters(tenant=_TENANT, valid_id_types=_VALID_ID_TYPES)

        expected_cluster = IdentityCluster(
            tenant=_TENANT,
            external_ids=(
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="A", id_type="T1"
                ),
            ),
            person_type=PersonType.JII,
            name=IdentityClusterName(tenant=_TENANT, given_name="John", surname="Doe"),
        )

        summaries = output | "Summarize" >> beam.Map(_cluster_summary)
        assert_that(
            summaries,
            equal_to(
                [
                    (
                        expected_cluster.identity_cluster_id,
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
        } | BuildIdentityClusters(tenant=_TENANT, valid_id_types=_VALID_ID_TYPES)

        summaries = output | "Summarize" >> beam.Map(_cluster_summary)

        expected_cluster = IdentityCluster(
            tenant=_TENANT,
            external_ids=(
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="A", id_type="T1"
                ),
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="B", id_type="T2"
                ),
            ),
            person_type=PersonType.JII,
            name=IdentityClusterName(tenant=_TENANT, given_name="John", surname="Doe"),
        )

        assert_that(
            summaries,
            equal_to(
                [
                    (
                        expected_cluster.identity_cluster_id,
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
        } | BuildIdentityClusters(tenant=_TENANT, valid_id_types=_VALID_ID_TYPES)

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
        } | BuildIdentityClusters(tenant=_TENANT, valid_id_types=_VALID_ID_TYPES)

        count = output | "Count" >> beam.combiners.Count.Globally()
        assert_that(count, equal_to([2]))
        self.test_pipeline.run()
