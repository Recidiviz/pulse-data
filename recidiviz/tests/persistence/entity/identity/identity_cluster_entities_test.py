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
"""Tests the entities defined in identity_cluster_entities.py."""
import datetime
import io
import pickle
import unittest
from typing import Type

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity_class,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    print_entity_tree,
)
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterEmail,
    IdentityClusterEthnicity,
    IdentityClusterExternalId,
    IdentityClusterGender,
    IdentityClusterName,
    IdentityClusterPhoneNumber,
    IdentityClusterRace,
    IdentityClusterSex,
)

_TENANT = "US_XX"

_EXTERNAL_ID = IdentityClusterExternalId(
    tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
)
_NAME = IdentityClusterName(
    tenant=_TENANT, given_name="John", surname="Doe", middle_name="Q", name_suffix="Jr"
)
_GENDER = IdentityClusterGender(tenant=_TENANT, gender=Gender.MALE)
_SEX = IdentityClusterSex(tenant=_TENANT, sex=Sex.MALE)
_RACE = IdentityClusterRace(tenant=_TENANT, race=Race.WHITE)
_ETHNICITY = IdentityClusterEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC)
_PHONE = IdentityClusterPhoneNumber(tenant=_TENANT, number="5550100001")
_EMAIL = IdentityClusterEmail(tenant=_TENANT, address="john@example.com")


class TestIdentityClusterExternalId(unittest.TestCase):
    """Tests the IdentityClusterExternalId entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
            ),
            IdentityClusterExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
            ),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
            ),
            IdentityClusterExternalId(
                tenant=_TENANT, external_id="EXT_002", id_type="US_XX_ID_TYPE"
            ),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_EXTERNAL_ID, pickle.loads(pickle.dumps(_EXTERNAL_ID)))


class TestIdentityClusterName(unittest.TestCase):
    """Tests the IdentityClusterName entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterName(tenant=_TENANT, given_name="John", surname="Doe"),
            IdentityClusterName(tenant=_TENANT, given_name="John", surname="Doe"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterName(tenant=_TENANT, given_name="John", surname="Doe"),
            IdentityClusterName(tenant=_TENANT, given_name="Jane", surname="Doe"),
        )

    def test_inequality_different_preferred_name(self) -> None:
        self.assertNotEqual(
            IdentityClusterName(
                tenant=_TENANT, given_name="John", preferred_name="Johnny"
            ),
            IdentityClusterName(tenant=_TENANT, given_name="John", preferred_name="JJ"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_NAME, pickle.loads(pickle.dumps(_NAME)))

    def test_rejects_digit_in_given_name(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            IdentityClusterName(tenant=_TENANT, given_name="J0hn", surname="Doe")

    def test_rejects_digit_in_surname(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            IdentityClusterName(tenant=_TENANT, given_name="John", surname="Do3")

    def test_rejects_overlong_name_suffix(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must be no more than"):
            IdentityClusterName(
                tenant=_TENANT,
                given_name="John",
                surname="Doe",
                name_suffix="Father of John",
            )

    def test_allows_numeric_name_suffix(self) -> None:
        name = IdentityClusterName(
            tenant=_TENANT, given_name="John", surname="Doe", name_suffix="3rd"
        )
        self.assertEqual(name.name_suffix, "3rd")


class TestIdentityClusterGender(unittest.TestCase):
    """Tests the IdentityClusterGender entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterGender(tenant=_TENANT, gender=Gender.MALE),
            IdentityClusterGender(tenant=_TENANT, gender=Gender.MALE),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterGender(tenant=_TENANT, gender=Gender.MALE),
            IdentityClusterGender(tenant=_TENANT, gender=Gender.FEMALE),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityClusterGender(
                tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"
            ),
            IdentityClusterGender(
                tenant=_TENANT, gender=Gender.MALE, gender_raw_text="MALE"
            ),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_GENDER, pickle.loads(pickle.dumps(_GENDER)))


class TestIdentityClusterSex(unittest.TestCase):
    """Tests the IdentityClusterSex entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterSex(tenant=_TENANT, sex=Sex.MALE),
            IdentityClusterSex(tenant=_TENANT, sex=Sex.MALE),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterSex(tenant=_TENANT, sex=Sex.MALE),
            IdentityClusterSex(tenant=_TENANT, sex=Sex.FEMALE),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityClusterSex(tenant=_TENANT, sex=Sex.MALE, sex_raw_text="M"),
            IdentityClusterSex(tenant=_TENANT, sex=Sex.MALE, sex_raw_text="MALE"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_SEX, pickle.loads(pickle.dumps(_SEX)))


class TestIdentityClusterRace(unittest.TestCase):
    """Tests the IdentityClusterRace entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterRace(tenant=_TENANT, race=Race.WHITE),
            IdentityClusterRace(tenant=_TENANT, race=Race.WHITE),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterRace(tenant=_TENANT, race=Race.WHITE),
            IdentityClusterRace(tenant=_TENANT, race=Race.BLACK),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityClusterRace(tenant=_TENANT, race=Race.WHITE, race_raw_text="W"),
            IdentityClusterRace(tenant=_TENANT, race=Race.WHITE, race_raw_text="WHITE"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_RACE, pickle.loads(pickle.dumps(_RACE)))


class TestIdentityClusterEthnicity(unittest.TestCase):
    """Tests the IdentityClusterEthnicity entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
            IdentityClusterEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
            IdentityClusterEthnicity(tenant=_TENANT, ethnicity=Ethnicity.NOT_HISPANIC),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityClusterEthnicity(
                tenant=_TENANT, ethnicity=Ethnicity.HISPANIC, ethnicity_raw_text="H"
            ),
            IdentityClusterEthnicity(
                tenant=_TENANT,
                ethnicity=Ethnicity.HISPANIC,
                ethnicity_raw_text="HISPANIC",
            ),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_ETHNICITY, pickle.loads(pickle.dumps(_ETHNICITY)))


class TestIdentityClusterPhoneNumber(unittest.TestCase):
    """Tests the IdentityClusterPhoneNumber entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterPhoneNumber(tenant=_TENANT, number="5550100001"),
            IdentityClusterPhoneNumber(tenant=_TENANT, number="5550100001"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterPhoneNumber(tenant=_TENANT, number="5550100001"),
            IdentityClusterPhoneNumber(tenant=_TENANT, number="5550199999"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_PHONE, pickle.loads(pickle.dumps(_PHONE)))


class TestIdentityClusterEmail(unittest.TestCase):
    """Tests the IdentityClusterEmail entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityClusterEmail(tenant=_TENANT, address="a@b.com"),
            IdentityClusterEmail(tenant=_TENANT, address="a@b.com"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityClusterEmail(tenant=_TENANT, address="a@b.com"),
            IdentityClusterEmail(tenant=_TENANT, address="c@b.com"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_EMAIL, pickle.loads(pickle.dumps(_EMAIL)))


class TestIdentityCluster(unittest.TestCase):
    """Tests the IdentityCluster root entity."""

    def _make_cluster(
        self,
        *,
        external_ids: tuple[IdentityClusterExternalId, ...] | None = None,
        birthdate: datetime.date | None = None,
        races: tuple[IdentityClusterRace, ...] | None = None,
    ) -> IdentityCluster:
        return IdentityCluster(
            tenant=_TENANT,
            person_type=PersonType.JII,
            birthdate=birthdate,
            external_ids=external_ids
            or (
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
                ),
            ),
            races=races or (),
        )

    def test_equality(self) -> None:
        def _make() -> IdentityCluster:
            return IdentityCluster(
                tenant=_TENANT,
                person_type=PersonType.JII,
                birthdate=datetime.date(1990, 1, 1),
                external_ids=(
                    IdentityClusterExternalId(
                        tenant=_TENANT,
                        external_id="EXT_001",
                        id_type="US_XX_ID_TYPE",
                    ),
                ),
                name=_NAME,
                gender=IdentityClusterGender(tenant=_TENANT, gender=Gender.MALE),
                races=(IdentityClusterRace(tenant=_TENANT, race=Race.WHITE),),
                phone_numbers=(
                    IdentityClusterPhoneNumber(tenant=_TENANT, number="5550100001"),
                ),
                emails=(
                    IdentityClusterEmail(tenant=_TENANT, address="john@example.com"),
                ),
            )

        self.assertEqual(_make(), _make())

    def test_inequality_different_external_id(self) -> None:
        self.assertNotEqual(
            self._make_cluster(
                external_ids=(
                    IdentityClusterExternalId(
                        tenant=_TENANT,
                        external_id="EXT_001",
                        id_type="US_XX_ID_TYPE",
                    ),
                )
            ),
            self._make_cluster(
                external_ids=(
                    IdentityClusterExternalId(
                        tenant=_TENANT,
                        external_id="EXT_002",
                        id_type="US_XX_ID_TYPE",
                    ),
                )
            ),
        )

    def test_inequality_different_birthdate(self) -> None:
        self.assertNotEqual(
            self._make_cluster(birthdate=datetime.date(1990, 1, 1)),
            self._make_cluster(birthdate=datetime.date(1991, 1, 1)),
        )

    def test_defaults(self) -> None:
        cluster = self._make_cluster()
        self.assertEqual(cluster.tenant, _TENANT)
        self.assertEqual(cluster.person_type, PersonType.JII)
        self.assertIsNone(cluster.birthdate)
        self.assertIsNone(cluster.name)
        self.assertIsNone(cluster.gender)
        self.assertIsNone(cluster.sex)
        self.assertEqual(cluster.races, ())
        self.assertIsNone(cluster.ethnicity)
        self.assertEqual(cluster.phone_numbers, ())
        self.assertEqual(cluster.emails, ())

    def test_pickle_roundtrip(self) -> None:
        cluster = IdentityCluster(
            tenant=_TENANT,
            person_type=PersonType.JII,
            birthdate=datetime.date(1990, 1, 1),
            external_ids=(
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
                ),
            ),
            name=_NAME,
            gender=IdentityClusterGender(tenant=_TENANT, gender=Gender.MALE),
            races=(
                IdentityClusterRace(tenant=_TENANT, race=Race.WHITE),
                IdentityClusterRace(
                    tenant=_TENANT, race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE
                ),
            ),
            phone_numbers=(
                IdentityClusterPhoneNumber(tenant=_TENANT, number="5550100001"),
            ),
            emails=(IdentityClusterEmail(tenant=_TENANT, address="john@example.com"),),
        )
        self.assertEqual(cluster, pickle.loads(pickle.dumps(cluster)))

    def test_id_and_hash_are_sha256_hex(self) -> None:
        cluster = self._make_cluster()
        self.assertEqual(64, len(cluster.identity_cluster_id))
        self.assertEqual(64, len(cluster.cluster_hash))
        int(cluster.identity_cluster_id, 16)
        int(cluster.cluster_hash, 16)

    def test_id_depends_only_on_external_ids(self) -> None:
        cluster_a = self._make_cluster(birthdate=datetime.date(1990, 1, 1))
        cluster_b = self._make_cluster(birthdate=datetime.date(2000, 1, 1))
        self.assertEqual(cluster_a.identity_cluster_id, cluster_b.identity_cluster_id)
        self.assertNotEqual(cluster_a.cluster_hash, cluster_b.cluster_hash)

    def test_id_is_stable_under_external_id_reordering(self) -> None:
        eid_a = IdentityClusterExternalId(
            tenant=_TENANT, external_id="EXT_A", id_type="US_XX_ID_TYPE"
        )
        eid_b = IdentityClusterExternalId(
            tenant=_TENANT, external_id="EXT_B", id_type="US_XX_ID_TYPE"
        )
        cluster_a = self._make_cluster(external_ids=(eid_a, eid_b))
        cluster_b = self._make_cluster(external_ids=(eid_b, eid_a))
        self.assertEqual(cluster_a.identity_cluster_id, cluster_b.identity_cluster_id)
        self.assertEqual(cluster_a.cluster_hash, cluster_b.cluster_hash)

    def test_hash_is_stable_under_race_reordering(self) -> None:
        race_black = IdentityClusterRace(tenant=_TENANT, race=Race.BLACK)
        race_white = IdentityClusterRace(tenant=_TENANT, race=Race.WHITE)
        cluster_a = self._make_cluster(races=(race_black, race_white))
        cluster_b = self._make_cluster(races=(race_white, race_black))
        self.assertEqual(cluster_a.cluster_hash, cluster_b.cluster_hash)

    def test_tenant_mismatch_in_external_id_raises(self) -> None:
        eid_other = IdentityClusterExternalId(
            tenant="US_YY", external_id="EXT_001", id_type="US_YY_ID_TYPE"
        )
        with self.assertRaisesRegex(ValueError, "mismatched tenants"):
            self._make_cluster(external_ids=(eid_other,))

    def test_tenant_mismatch_in_child_entity_raises(self) -> None:
        race_other = IdentityClusterRace(tenant="US_YY", race=Race.WHITE)
        with self.assertRaisesRegex(ValueError, "mismatched tenants"):
            self._make_cluster(races=(race_other,))

    def test_child_back_edges_point_to_cluster(self) -> None:
        race = IdentityClusterRace(tenant=_TENANT, race=Race.WHITE)
        external_id = IdentityClusterExternalId(
            tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
        )
        cluster = self._make_cluster(
            external_ids=(external_id,),
            races=(race,),
        )
        self.assertIs(cluster.races[0].identity_cluster, cluster)
        self.assertIs(cluster.external_ids[0].identity_cluster, cluster)

    def test_hash_is_stable(self) -> None:
        cluster = IdentityCluster(
            tenant=_TENANT,
            person_type=PersonType.JII,
            birthdate=datetime.date(1990, 1, 1),
            external_ids=(
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
                ),
                IdentityClusterExternalId(
                    tenant=_TENANT, external_id="EXT_002", id_type="US_XX_ID_TYPE"
                ),
            ),
            name=IdentityClusterName(tenant=_TENANT, given_name="John", surname="Doe"),
            gender=IdentityClusterGender(tenant=_TENANT, gender=Gender.MALE),
            races=(
                IdentityClusterRace(tenant=_TENANT, race=Race.BLACK, race_raw_text="B"),
            ),
        )
        self.assertEqual(
            cluster.identity_cluster_id,
            "407efabf4ca809a7154543ab3086e8006bd4cdc4d17fd46325bf2dd437d33930",
        )
        self.assertEqual(
            cluster.cluster_hash,
            "ee7a29d22e9de4bd2cfcf89c6e4a055617936b29805ee9f5a5e6a76849882c4f",
        )

    def test_print_entity_tree_handles_tuple_forward_edges(self) -> None:
        cluster = self._make_cluster(
            races=(
                IdentityClusterRace(tenant=_TENANT, race=Race.BLACK),
                IdentityClusterRace(tenant=_TENANT, race=Race.WHITE),
            ),
        )
        buf = io.StringIO()
        print_entity_tree(
            cluster,
            entities_module_context_for_module(identity_cluster_entities),
            file_or_buffer=buf,
        )
        output = buf.getvalue()
        self.assertIn("IdentityCluster", output)
        self.assertIn("IdentityClusterRace", output)
        self.assertIn("IdentityClusterExternalId", output)


# Fragment classes that are not represented in the cluster tree: the root and
# the attributes intermediate (whose flat fields move onto IdentityCluster).
_FRAGMENT_ONLY_CLASSES = {
    identity_fragment_entities.IdentityFragment,
    identity_fragment_entities.IdentityAttributes,
}

# Cluster-only flat fields that have no fragment counterpart.
_CLUSTER_ONLY_FIELDS = {"identity_cluster_id", "cluster_hash"}


def _fragment_counterpart_of(cluster_cls: Type[Entity]) -> Type[Entity]:
    fragment_name = cluster_cls.__name__.replace("IdentityCluster", "Identity", 1)
    return getattr(identity_fragment_entities, fragment_name)


def _flat_fields(cls: Type[Entity]) -> dict[str, attr.Attribute]:
    """Returns the flat (non-Entity-reference) fields on `cls`."""
    field_index = entities_module_context_for_entity_class(cls).field_index()
    flat_field_names = field_index.get_all_entity_fields(
        cls, EntityFieldType.FLAT_FIELD
    )
    attrs_by_name = attr.fields_dict(cls)
    return {name: attrs_by_name[name] for name in flat_field_names}


def _forward_edge_field_names(cls: Type[Entity]) -> set[str]:
    """Returns the forward-edge field names on `cls`."""
    field_index = entities_module_context_for_entity_class(cls).field_index()
    return field_index.get_all_entity_fields(cls, EntityFieldType.FORWARD_EDGE)


def _validator_repr(validator: object) -> str:
    """Stable string representation of an attr validator for equality
    comparison across fragment/cluster pairs."""
    return repr(validator)


class TestClusterEntitiesParity(unittest.TestCase):
    """Verifies parity between fragment and cluster entity trees."""

    def setUp(self) -> None:
        self.fragment_classes = get_all_entity_classes_in_module(
            identity_fragment_entities
        )
        self.cluster_classes = get_all_entity_classes_in_module(
            identity_cluster_entities
        )
        self.cluster_classes_by_name = {c.__name__: c for c in self.cluster_classes}

    def test_every_clustered_fragment_class_has_cluster_counterpart(self) -> None:
        for fragment_cls in self.fragment_classes:
            if fragment_cls in _FRAGMENT_ONLY_CLASSES:
                continue
            expected_cluster_name = fragment_cls.__name__.replace(
                "Identity", "IdentityCluster", 1
            )
            self.assertIn(
                expected_cluster_name,
                self.cluster_classes_by_name,
                f"Expected cluster entity [{expected_cluster_name}] corresponding "
                f"to fragment entity [{fragment_cls.__name__}].",
            )

    def test_every_cluster_class_has_fragment_counterpart_or_is_root(self) -> None:
        for cluster_cls in self.cluster_classes:
            if cluster_cls is identity_cluster_entities.IdentityCluster:
                continue
            fragment_name = cluster_cls.__name__.replace(
                "IdentityCluster", "Identity", 1
            )
            self.assertTrue(
                hasattr(identity_fragment_entities, fragment_name),
                f"Cluster entity [{cluster_cls.__name__}] has no corresponding "
                f"fragment entity [{fragment_name}].",
            )

    def test_flat_field_parity_for_child_entities(self) -> None:
        for cluster_cls in self.cluster_classes:
            if cluster_cls is identity_cluster_entities.IdentityCluster:
                continue
            fragment_cls = _fragment_counterpart_of(cluster_cls)
            self._assert_flat_fields_match(fragment_cls, cluster_cls)

    def test_identity_cluster_carries_attributes_flat_fields(self) -> None:
        self._assert_flat_fields_match(
            identity_fragment_entities.IdentityAttributes,
            identity_cluster_entities.IdentityCluster,
            extra_cluster_fields=_CLUSTER_ONLY_FIELDS,
        )

    def test_identity_cluster_has_cluster_only_fields(self) -> None:
        cluster_fields = attr.fields_dict(identity_cluster_entities.IdentityCluster)
        for field_name in _CLUSTER_ONLY_FIELDS:
            self.assertIn(field_name, cluster_fields)
            self.assertEqual(
                cluster_fields[field_name].type,
                str,
                f"Expected [{field_name}] on IdentityCluster to be typed `str`.",
            )

    def test_every_child_cluster_entity_has_identity_cluster_back_edge(self) -> None:
        for cluster_cls in self.cluster_classes:
            if cluster_cls is identity_cluster_entities.IdentityCluster:
                continue
            fields = attr.fields_dict(cluster_cls)
            self.assertIn(
                "identity_cluster",
                fields,
                f"Child cluster entity [{cluster_cls.__name__}] is missing the "
                f"`identity_cluster` back-edge.",
            )

    def test_all_fields_immutable_except_back_edge_and_computed(self) -> None:
        """Verifies that all fields in every cluster entity have been frozen
        (using `on_setattr=attr.setters.frozen`), with the exception of each
        entity's `identity_cluster` back-edge (set by `set_backedges`) and
        `IdentityCluster.identity_cluster_id` and `IdentityCluster.cluster_hash`
        (set in `__attrs_post_init__`).
        """
        for cluster_cls in self.cluster_classes:
            is_root = cluster_cls is identity_cluster_entities.IdentityCluster
            mutable_fields = (
                set(_CLUSTER_ONLY_FIELDS)
                if is_root
                else {identity_cluster_entities.IdentityCluster.back_edge_field_name()}
            )
            # Probe an uninitialized instance so we don't need valid constructor
            # arguments for every class; the frozen setter raises before any
            # field value is read.
            instance = object.__new__(cluster_cls)
            for field in attr.fields(cluster_cls):
                if field.name in mutable_fields:
                    self.assertIs(
                        field.on_setattr,
                        attr.setters.validate,
                        f"{cluster_cls.__name__}.{field.name} must declare "
                        f"on_setattr=attr.setters.validate so the framework can "
                        f"set it and any validator still runs.",
                    )
                    continue
                with self.assertRaises(
                    attr.exceptions.FrozenAttributeError,
                    msg=f"{cluster_cls.__name__}.{field.name} must be immutable.",
                ):
                    setattr(instance, field.name, None)

    def test_identity_cluster_is_a_root_entity(self) -> None:
        self.assertTrue(
            issubclass(identity_cluster_entities.IdentityCluster, RootEntity)
        )
        self.assertEqual(
            "identity_cluster",
            identity_cluster_entities.IdentityCluster.back_edge_field_name(),
        )

    def test_identity_cluster_forward_edges_match_flattened_fragment_tree(
        self,
    ) -> None:
        # The flattened root's forward edges should be the union of
        # IdentityFragment's forward edges and IdentityAttributes's forward
        # edges.
        expected_forward_edges = _forward_edge_field_names(
            identity_fragment_entities.IdentityFragment
        ) - {"attributes"} | _forward_edge_field_names(
            identity_fragment_entities.IdentityAttributes
        )
        cluster_forward_edges = _forward_edge_field_names(
            identity_cluster_entities.IdentityCluster
        )
        self.assertEqual(expected_forward_edges, cluster_forward_edges)

    def test_module_is_registered_with_factory(self) -> None:
        context = entities_module_context_for_module(identity_cluster_entities)
        self.assertEqual(identity_cluster_entities, context.entities_module())
        self.assertEqual(
            {c.__name__ for c in self.cluster_classes},
            set(context.class_hierarchy()),
        )

    def _assert_flat_fields_match(
        self,
        fragment_cls: Type[Entity],
        cluster_cls: Type[Entity],
        extra_cluster_fields: set[str] | None = None,
    ) -> None:
        """Asserts every flat field on `fragment_cls` is present on
        `cluster_cls` with the same type and validator, allowing the cluster
        class to declare additional flat fields listed in
        `extra_cluster_fields`."""
        extra_cluster_fields = extra_cluster_fields or set()
        fragment_flat = _flat_fields(fragment_cls)
        cluster_flat = _flat_fields(cluster_cls)

        missing = set(fragment_flat) - set(cluster_flat)
        self.assertFalse(
            missing,
            f"Flat fields on [{fragment_cls.__name__}] missing from "
            f"[{cluster_cls.__name__}]: {missing}",
        )

        unexpected = set(cluster_flat) - set(fragment_flat) - extra_cluster_fields
        self.assertFalse(
            unexpected,
            f"Cluster class [{cluster_cls.__name__}] has unexpected flat fields "
            f"that aren't on [{fragment_cls.__name__}]: {unexpected}",
        )

        for field_name, fragment_field in fragment_flat.items():
            cluster_field = cluster_flat[field_name]
            self.assertEqual(
                fragment_field.type,
                cluster_field.type,
                f"Field [{field_name}] has type "
                f"[{cluster_field.type}] on [{cluster_cls.__name__}] but "
                f"[{fragment_field.type}] on [{fragment_cls.__name__}].",
            )
            self.assertEqual(
                _validator_repr(fragment_field.validator),
                _validator_repr(cluster_field.validator),
                f"Field [{field_name}] has different validators on "
                f"[{fragment_cls.__name__}] and [{cluster_cls.__name__}].",
            )
