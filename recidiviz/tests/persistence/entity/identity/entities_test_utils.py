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
"""Test utils for generating identity ingest pipeline `Entity` trees."""
import datetime

from recidiviz.common.constants.identity import NameUse, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.entity_utils import (
    set_backedges_allowing_intermediate_entities,
)
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities_module_context import (
    IDENTITY_FRAGMENT_ENTITIES_CONTEXT,
)

_TENANT = Tenant.US_XX


def identity_fragment_for_test(
    *,
    external_ids: list[tuple[str, str]],
    tenant: Tenant = Tenant.US_XX,
    person_type: PersonType = PersonType.JII,
    name: identity_fragment_entities.IdentityName | None = None,
    birthdate: datetime.date | None = None,
    gender: identity_fragment_entities.IdentityGender | None = None,
    sex: identity_fragment_entities.IdentitySex | None = None,
    ethnicity: identity_fragment_entities.IdentityEthnicity | None = None,
    races: list[identity_fragment_entities.IdentityRace] | None = None,
    phone_numbers: list[identity_fragment_entities.IdentityPhoneNumber] | None = None,
    emails: list[identity_fragment_entities.IdentityEmail] | None = None,
) -> identity_fragment_entities.IdentityFragment:
    """Returns a test `IdentityFragment` with `IdentityExternalId` children built
    from the given (`external_id`, `id_type`) pairs.

    If no attribute argument is provided, the fragment is constructed as an
    external-id-only carrier (`attributes=None`, since `IdentityAttributes`
    requires at least one attribute beyond `tenant`); otherwise the attribute
    arguments are wrapped in an `IdentityAttributes` child.
    """
    attributes: identity_fragment_entities.IdentityAttributes | None = None
    if any((name, birthdate, gender, sex, ethnicity, races, phone_numbers, emails)):
        attributes = identity_fragment_entities.IdentityAttributes(
            tenant=tenant,
            name=name,
            birthdate=birthdate,
            gender=gender,
            sex=sex,
            ethnicity=ethnicity,
            races=races or [],
            phone_numbers=phone_numbers or [],
            emails=emails or [],
        )
    return identity_fragment_entities.IdentityFragment(
        tenant=tenant,
        external_ids=[
            identity_fragment_entities.IdentityExternalId(
                tenant=tenant, external_id=external_id, id_type=id_type
            )
            for external_id, id_type in external_ids
        ],
        person_type=person_type,
        attributes=attributes,
    )


def generate_full_graph_identity_fragment(
    set_back_edges: bool,
) -> identity_fragment_entities.IdentityFragment:
    """Test util for generating an `IdentityFragment` that has at least one
    child of each possible fragment-entity type, with all possible edge types
    defined between objects.

    Args:
        set_back_edges: if true, populates the back edges linking each entity in
            the fragment to its parent and, through the tree, to the root fragment.
            If false, returns the fragment with its back edges unset.

    Returns:
        A test instance of an `IdentityFragment`.
    """
    attributes = identity_fragment_entities.IdentityAttributes(
        tenant=_TENANT,
        birthdate=datetime.date(1990, 1, 1),
        name=identity_fragment_entities.IdentityName(
            tenant=_TENANT,
            given_name="John",
            preferred_name="Johnny",
            surname="Doe",
            middle_name="Quincy",
            name_suffix="Jr",
        ),
        gender=identity_fragment_entities.IdentityGender(
            tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"
        ),
        sex=identity_fragment_entities.IdentitySex(
            tenant=_TENANT, sex=Sex.MALE, sex_raw_text="M"
        ),
        ethnicity=identity_fragment_entities.IdentityEthnicity(
            tenant=_TENANT,
            ethnicity=Ethnicity.NOT_HISPANIC,
            ethnicity_raw_text="NH",
        ),
        races=[
            identity_fragment_entities.IdentityRace(
                tenant=_TENANT, race=Race.BLACK, race_raw_text="B"
            ),
            identity_fragment_entities.IdentityRace(
                tenant=_TENANT, race=Race.WHITE, race_raw_text="W"
            ),
        ],
        phone_numbers=[
            identity_fragment_entities.IdentityPhoneNumber(
                tenant=_TENANT, number="5550100001"
            ),
            identity_fragment_entities.IdentityPhoneNumber(
                tenant=_TENANT, number="5550100002"
            ),
        ],
        emails=[
            identity_fragment_entities.IdentityEmail(
                tenant=_TENANT, address="a@example.com"
            ),
            identity_fragment_entities.IdentityEmail(
                tenant=_TENANT, address="b@example.com"
            ),
        ],
        aliases=[
            identity_fragment_entities.IdentityAlias(
                tenant=_TENANT,
                given_name="Johnny",
                surname="Doe",
                middle_name="Q",
                name_suffix="Jr",
                name_use=NameUse.ALIAS,
                name_use_raw_text="AKA",
            ),
            identity_fragment_entities.IdentityAlias(
                tenant=_TENANT,
                given_name="Jack",
                surname="Smith",
                middle_name="Quentin",
                name_suffix="Sr",
                name_use=NameUse.FORMER,
                name_use_raw_text="MAIDEN",
            ),
        ],
    )

    fragment = identity_fragment_entities.IdentityFragment(
        tenant=_TENANT,
        external_ids=[
            identity_fragment_entities.IdentityExternalId(
                tenant=_TENANT,
                external_id="EXT_001",
                id_type=f"{_TENANT.value}_ID_TYPE",
            ),
            identity_fragment_entities.IdentityExternalId(
                tenant=_TENANT,
                external_id="EXT_002",
                id_type=f"{_TENANT.value}_ID_TYPE",
            ),
        ],
        person_type=PersonType.JII,
        attributes=attributes,
    )

    if set_back_edges:
        set_backedges_allowing_intermediate_entities(
            fragment, IDENTITY_FRAGMENT_ENTITIES_CONTEXT
        )

    return fragment


def generate_full_graph_identity_cluster() -> identity_cluster_entities.IdentityCluster:
    """Test util for generating an `IdentityCluster` that has at least one
    child of each possible cluster-entity type, with all possible edge types
    defined between objects.

    Back-edges are wired automatically by `IdentityCluster.__attrs_post_init__`.

    Returns:
        A test instance of an `IdentityCluster`.
    """
    cluster = identity_cluster_entities.IdentityCluster(
        tenant=_TENANT,
        person_type=PersonType.JII,
        birthdate=datetime.date(1990, 1, 1),
        external_ids=(
            identity_cluster_entities.IdentityClusterExternalId(
                tenant=_TENANT,
                external_id="EXT_001",
                id_type=f"{_TENANT.value}_ID_TYPE",
            ),
            identity_cluster_entities.IdentityClusterExternalId(
                tenant=_TENANT,
                external_id="EXT_002",
                id_type=f"{_TENANT.value}_ID_TYPE",
            ),
        ),
        name=identity_cluster_entities.IdentityClusterName(
            tenant=_TENANT,
            given_name="John",
            preferred_name="Johnny",
            surname="Doe",
            middle_name="Quincy",
            name_suffix="Jr",
        ),
        gender=identity_cluster_entities.IdentityClusterGender(
            tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"
        ),
        sex=identity_cluster_entities.IdentityClusterSex(
            tenant=_TENANT, sex=Sex.MALE, sex_raw_text="M"
        ),
        ethnicity=identity_cluster_entities.IdentityClusterEthnicity(
            tenant=_TENANT,
            ethnicity=Ethnicity.NOT_HISPANIC,
            ethnicity_raw_text="NH",
        ),
        races=(
            identity_cluster_entities.IdentityClusterRace(
                tenant=_TENANT, race=Race.BLACK, race_raw_text="B"
            ),
            identity_cluster_entities.IdentityClusterRace(
                tenant=_TENANT, race=Race.WHITE, race_raw_text="W"
            ),
        ),
        phone_numbers=(
            identity_cluster_entities.IdentityClusterPhoneNumber(
                tenant=_TENANT, number="5550100001"
            ),
            identity_cluster_entities.IdentityClusterPhoneNumber(
                tenant=_TENANT, number="5550100002"
            ),
        ),
        emails=(
            identity_cluster_entities.IdentityClusterEmail(
                tenant=_TENANT, address="a@example.com"
            ),
            identity_cluster_entities.IdentityClusterEmail(
                tenant=_TENANT, address="b@example.com"
            ),
        ),
        aliases=(
            identity_cluster_entities.IdentityClusterAlias(
                tenant=_TENANT,
                given_name="Johnny",
                surname="Doe",
                middle_name="Q",
                name_suffix="Jr",
                name_use=NameUse.ALIAS,
                name_use_raw_text="AKA",
            ),
            identity_cluster_entities.IdentityClusterAlias(
                tenant=_TENANT,
                given_name="Jack",
                surname="Smith",
                middle_name="Quentin",
                name_suffix="Sr",
                name_use=NameUse.FORMER,
                name_use_raw_text="MAIDEN",
            ),
        ),
    )

    return cluster
