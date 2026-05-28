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

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)

_TENANT = "US_XX"


def generate_full_graph_identity_fragment(
    set_back_edges: bool,
) -> identity_fragment_entities.IdentityFragment:
    """Test util for generating an `IdentityFragment` that has at least one
    child of each possible fragment-entity type, with all possible edge types
    defined between objects.

    Args:
        set_back_edges: explicitly sets the direct-parent back edges on the
            graph (`IdentityExternalId.fragment` and `IdentityAttributes.fragment`
            point to the root; the leaf entities under `IdentityAttributes`
            point back via `identity_attributes`). Unlike the activity pattern,
            the leaf entities do not have a back edge to the root
            `IdentityFragment` because the fragment tree is two-level rather
            than flat, so the generic `set_backedges` helper is not used here.

    Returns:
        A test instance of an `IdentityFragment`.
    """
    attributes = identity_fragment_entities.IdentityAttributes(
        tenant=_TENANT,
        person_type=PersonType.JII,
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
    )

    fragment = identity_fragment_entities.IdentityFragment(
        tenant=_TENANT,
        external_ids=[
            identity_fragment_entities.IdentityExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type=f"{_TENANT}_ID_TYPE"
            ),
            identity_fragment_entities.IdentityExternalId(
                tenant=_TENANT, external_id="EXT_002", id_type=f"{_TENANT}_ID_TYPE"
            ),
        ],
        attributes=attributes,
    )

    if set_back_edges:
        for external_id in fragment.external_ids:
            external_id.fragment = fragment
        fragment.attributes.fragment = fragment
        for singular_field in ("name", "gender", "sex", "ethnicity"):
            child = getattr(fragment.attributes, singular_field)
            if child is not None:
                child.identity_attributes = fragment.attributes
        for list_field in ("races", "phone_numbers", "emails"):
            for child in getattr(fragment.attributes, list_field):
                child.identity_attributes = fragment.attributes

    return fragment


def generate_full_graph_identity_cluster(
    set_back_edges: bool,
) -> identity_cluster_entities.IdentityCluster:
    """Test util for generating an `IdentityCluster` that has at least one
    child of each possible cluster-entity type, with all possible edge types
    defined between objects.

    Args:
        set_back_edges: explicitly sets all the back edges on the graph.

    Returns:
        A test instance of an `IdentityCluster`.
    """
    cluster = identity_cluster_entities.IdentityCluster(
        tenant=_TENANT,
        person_type=PersonType.JII,
        birthdate=datetime.date(1990, 1, 1),
        external_ids=[
            identity_cluster_entities.IdentityClusterExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type=f"{_TENANT}_ID_TYPE"
            ),
            identity_cluster_entities.IdentityClusterExternalId(
                tenant=_TENANT, external_id="EXT_002", id_type=f"{_TENANT}_ID_TYPE"
            ),
        ],
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
        races=[
            identity_cluster_entities.IdentityClusterRace(
                tenant=_TENANT, race=Race.BLACK, race_raw_text="B"
            ),
            identity_cluster_entities.IdentityClusterRace(
                tenant=_TENANT, race=Race.WHITE, race_raw_text="W"
            ),
        ],
        phone_numbers=[
            identity_cluster_entities.IdentityClusterPhoneNumber(
                tenant=_TENANT, number="5550100001"
            ),
            identity_cluster_entities.IdentityClusterPhoneNumber(
                tenant=_TENANT, number="5550100002"
            ),
        ],
        emails=[
            identity_cluster_entities.IdentityClusterEmail(
                tenant=_TENANT, address="a@example.com"
            ),
            identity_cluster_entities.IdentityClusterEmail(
                tenant=_TENANT, address="b@example.com"
            ),
        ],
    )

    if set_back_edges:
        set_backedges(
            cluster, entities_module_context_for_module(identity_cluster_entities)
        )

    return cluster
