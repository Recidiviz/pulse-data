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
import datetime
import pickle
import unittest

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.identity import (
    identity_fragment_entities as identity_entities_module,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityEmail,
    IdentityEthnicity,
    IdentityExternalId,
    IdentityGender,
    IdentityName,
    IdentityPhoneNumber,
    IdentityRace,
    IdentitySex,
)
from recidiviz.persistence.entity.serialization import serialize_entity_tree_into_json
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


def _eid(external_id: str, id_type: str, tenant: str = _TENANT) -> IdentityExternalId:
    return IdentityExternalId(tenant=tenant, external_id=external_id, id_type=id_type)


def _cluster(
    eids: list[IdentityExternalId],
    attrs: IdentityAttributes = _ATTRIBUTES,
) -> IdentityCluster:
    return IdentityCluster(external_ids=eids, attributes=attrs)


def _fully_populated_attributes() -> IdentityAttributes:
    """Constructs an IdentityAttributes with every field populated, so the
    golden hash test below covers every serializable field."""
    return IdentityAttributes(
        tenant=_TENANT,
        person_type=PersonType.JII,
        name=IdentityName(
            tenant=_TENANT,
            given_name="John",
            preferred_name="Johnny",
            surname="Doe",
            middle_name="Quincy",
            name_suffix="Jr",
        ),
        birthdate=datetime.date(1990, 1, 1),
        gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"),
        sex=IdentitySex(tenant=_TENANT, sex=Sex.MALE, sex_raw_text="M"),
        ethnicity=IdentityEthnicity(
            tenant=_TENANT,
            ethnicity=Ethnicity.NOT_HISPANIC,
            ethnicity_raw_text="NH",
        ),
        races=[
            IdentityRace(tenant=_TENANT, race=Race.BLACK, race_raw_text="B"),
            IdentityRace(tenant=_TENANT, race=Race.WHITE, race_raw_text="W"),
        ],
        phone_numbers=[
            IdentityPhoneNumber(tenant=_TENANT, number="5550100001"),
            IdentityPhoneNumber(tenant=_TENANT, number="5550100002"),
        ],
        emails=[
            IdentityEmail(tenant=_TENANT, address="a@example.com"),
            IdentityEmail(tenant=_TENANT, address="b@example.com"),
        ],
    )


class TestIdentityCluster(unittest.TestCase):
    """Tests the IdentityCluster data class."""

    def test_construction_derives_id_and_hash(self) -> None:
        cluster = IdentityCluster(
            external_ids=[_EXTERNAL_ID],
            attributes=_ATTRIBUTES,
        )
        self.assertEqual(cluster.external_ids, [_EXTERNAL_ID])
        self.assertEqual(cluster.attributes, _ATTRIBUTES)
        self.assertEqual(len(cluster.cluster_id), 64)
        int(cluster.cluster_id, 16)  # Should not raise
        self.assertEqual(len(cluster.cluster_hash), 64)
        int(cluster.cluster_hash, 16)  # Should not raise

    def test_deterministic(self) -> None:
        c1 = IdentityCluster(external_ids=[_EXTERNAL_ID], attributes=_ATTRIBUTES)
        c2 = IdentityCluster(external_ids=[_EXTERNAL_ID], attributes=_ATTRIBUTES)
        self.assertEqual(c1.cluster_id, c2.cluster_id)
        self.assertEqual(c1.cluster_hash, c2.cluster_hash)

    def test_pickle_roundtrip(self) -> None:
        cluster = IdentityCluster(
            external_ids=[_EXTERNAL_ID],
            attributes=_ATTRIBUTES,
        )
        restored = pickle.loads(pickle.dumps(cluster))
        self.assertEqual(
            attr.asdict(cluster, recurse=False),
            attr.asdict(restored, recurse=False),
        )

    def test_mismatched_external_id_tenant_raises(self) -> None:
        attrs = IdentityAttributes(tenant="US_OZ", person_type=PersonType.JII)
        with self.assertRaisesRegex(ValueError, "mismatched tenants"):
            IdentityCluster(
                external_ids=[
                    _eid("A", "T1", tenant="US_OZ"),
                    _eid("B", "T2", tenant="US_XX"),
                ],
                attributes=attrs,
            )


class TestClusterId(unittest.TestCase):
    """Tests for cluster id derivation."""

    def test_deterministic(self) -> None:
        eids = [_eid("A", "T1"), _eid("B", "T2")]
        self.assertEqual(
            _cluster(eids).cluster_id,
            _cluster(eids).cluster_id,
        )

    def test_order_independent(self) -> None:
        eids_forward = [_eid("A", "T1"), _eid("B", "T2")]
        eids_reverse = [_eid("B", "T2"), _eid("A", "T1")]
        self.assertEqual(
            _cluster(eids_forward).cluster_id,
            _cluster(eids_reverse).cluster_id,
        )

    def test_tenant_namespacing(self) -> None:
        eids_oz = [_eid("A", "T1", tenant="US_OZ")]
        attrs_oz = IdentityAttributes(tenant="US_OZ", person_type=PersonType.JII)
        eids_xx = [_eid("A", "T1", tenant="US_XX")]
        attrs_xx = IdentityAttributes(tenant="US_XX", person_type=PersonType.JII)
        self.assertNotEqual(
            _cluster(eids_oz, attrs_oz).cluster_id,
            _cluster(eids_xx, attrs_xx).cluster_id,
        )

    def test_different_eids_different_ids(self) -> None:
        self.assertNotEqual(
            _cluster([_eid("A", "T1")]).cluster_id,
            _cluster([_eid("B", "T1")]).cluster_id,
        )

    def test_returns_hex_string(self) -> None:
        result = _cluster([_eid("A", "T1")]).cluster_id
        self.assertEqual(len(result), 64)
        int(result, 16)  # Should not raise


class TestClusterHash(unittest.TestCase):
    """Tests for cluster hash derivation."""

    def test_deterministic(self) -> None:
        eids = [_eid("A", "T1")]
        self.assertEqual(
            _cluster(eids).cluster_hash,
            _cluster(eids).cluster_hash,
        )

    def test_eid_order_independent(self) -> None:
        eids_forward = [_eid("A", "T1"), _eid("B", "T2")]
        eids_reverse = [_eid("B", "T2"), _eid("A", "T1")]
        self.assertEqual(
            _cluster(eids_forward).cluster_hash,
            _cluster(eids_reverse).cluster_hash,
        )

    def test_changes_on_eid_change(self) -> None:
        self.assertNotEqual(
            _cluster([_eid("A", "T1")]).cluster_hash,
            _cluster([_eid("A", "T1"), _eid("B", "T2")]).cluster_hash,
        )

    def test_changes_on_attribute_change(self) -> None:
        eids = [_eid("A", "T1")]
        attrs1 = IdentityAttributes(
            tenant=_TENANT,
            person_type=PersonType.JII,
            name=IdentityName(tenant=_TENANT, given_name="JOHN", surname="DOE"),
        )
        attrs2 = IdentityAttributes(
            tenant=_TENANT,
            person_type=PersonType.JII,
            name=IdentityName(tenant=_TENANT, given_name="JANE", surname="DOE"),
        )
        self.assertNotEqual(
            _cluster(eids, attrs1).cluster_hash,
            _cluster(eids, attrs2).cluster_hash,
        )

    def test_tenant_namespacing(self) -> None:
        eids_oz = [_eid("A", "T1", tenant="US_OZ")]
        attrs_oz = IdentityAttributes(tenant="US_OZ", person_type=PersonType.JII)
        eids_xx = [_eid("A", "T1", tenant="US_XX")]
        attrs_xx = IdentityAttributes(tenant="US_XX", person_type=PersonType.JII)
        self.assertNotEqual(
            _cluster(eids_oz, attrs_oz).cluster_hash,
            _cluster(eids_xx, attrs_xx).cluster_hash,
        )

    def test_returns_hex_string(self) -> None:
        result = _cluster([_eid("A", "T1")]).cluster_hash
        self.assertEqual(len(result), 64)
        int(result, 16)  # Should not raise

    def test_golden_hashes_for_fully_populated_cluster(self) -> None:
        """Locks cluster_id and cluster_hash to known values for a fully
        populated cluster.

        The hash is a contract with the Identity Service: it is used to detect
        whether a cluster has changed since the last import. If this test
        fails, do not just replace the expected hashes with whatever the code
        now produces. First confirm the change is intentional: if the
        serialization format changed unintentionally, every cluster will
        appear "updated" on the next run.

        If a new field was added to IdentityAttributes (or a sub-entity like
        IdentityName), test_fully_populated_attributes_field_coverage will
        fail first, pointing you to the unpopulated field. Populate it in
        _fully_populated_attributes, then update the expected hashes here.
        """
        cluster = IdentityCluster(
            external_ids=[
                _eid("EXT_A", "T1"),
                _eid("EXT_B", "T2"),
            ],
            attributes=_fully_populated_attributes(),
        )
        self.assertEqual(
            cluster.cluster_id,
            "7605918917d526fa6013fc571832cd9a943811e1c3183a5f0e459b4f0902a203",
        )
        self.assertEqual(
            cluster.cluster_hash,
            "932eb8ad7c6d05f47a0ad62124a5324f4505d5215ba2c5ee79c54f7f39f409a3",
        )

    # Fields that must remain None even in _fully_populated_attributes (their
    # validators enforce it). Listed here so the field-coverage check below
    # doesn't flag them.
    _FIELDS_ALWAYS_NONE: frozenset[str] = frozenset(
        {"IdentityAttributes.person_type_raw_text"}
    )

    def test_fully_populated_attributes_field_coverage(self) -> None:
        """Ensures _fully_populated_attributes populates every field so the
        golden hash test exercises the full serialization surface.

        If this test fails because a field is None or a list is empty, update
        _fully_populated_attributes to include a value for the new field, then
        update the expected hashes in the golden hash test above.
        """
        serialized = serialize_entity_tree_into_json(
            _fully_populated_attributes(),
            entities_module_context_for_module(identity_entities_module),
        )
        self._assert_all_fields_populated(serialized, "IdentityAttributes")

    def _assert_all_fields_populated(self, d: dict[str, object], path: str) -> None:
        for key, value in d.items():
            field_path = f"{path}.{key}"
            if field_path in self._FIELDS_ALWAYS_NONE:
                self.assertIsNone(
                    value,
                    f"{field_path} should be None per its validator, got {value!r}",
                )
                continue
            if isinstance(value, dict):
                self._assert_all_fields_populated(value, field_path)
            elif isinstance(value, list):
                self.assertTrue(
                    len(value) > 0,
                    f"{field_path} is empty in _fully_populated_attributes",
                )
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        self._assert_all_fields_populated(item, f"{field_path}[{i}]")
            else:
                self.assertIsNotNone(
                    value,
                    f"{field_path} is None in _fully_populated_attributes",
                )
