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
"""Entity classes for the post-clustering output of the batch identity
clustering pipeline.

These entities represent a fully clustered logical person: the union of one or
more `IdentityFragment` entities (defined in `identity_fragment_entities.py`)
that have been identified as referring to the same individual.

The cluster tree is intentionally flattened relative to the fragment tree; there
is no `IdentityClusterAttributes` intermediate. `person_type` and
`birthdate` live directly on the `IdentityCluster` root alongside all child
entities (name, gender, sex, races, ethnicity, phone numbers, emails).
"""
import datetime
import hashlib
import json
from typing import Any

import attr

from recidiviz.common.attr_validators import (
    is_none,
    is_opt,
    is_opt_str,
    is_opt_valid_name_part,
    is_opt_valid_name_suffix,
    is_tuple_of,
    is_valid_email,
    is_valid_phone_number,
)
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entities_from_tree,
    set_backedges,
)
from recidiviz.persistence.entity.identity.identity_cluster_entities_module_context import (
    IDENTITY_CLUSTER_ENTITIES_CONTEXT,
)
from recidiviz.persistence.entity.identity.identity_entity_mixin import (
    IdentityEntityMixin,
)
from recidiviz.persistence.entity.reasonable_date_validators import (
    REASONABLE_OPT_BIRTHDATE_VALIDATOR,
)
from recidiviz.persistence.entity.serialization import serialize_entity_tree_into_json


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterExternalId(IdentityEntityMixin, ExternalIdEntity):
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterName(IdentityEntityMixin, Entity):
    given_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    preferred_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    surname: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    middle_name: str | None = attr.ib(default=None, validator=is_opt_valid_name_part)
    name_suffix: str | None = attr.ib(default=None, validator=is_opt_valid_name_suffix)
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterGender(IdentityEntityMixin, EnumEntity):
    gender: Gender = attr.ib(validator=attr.validators.instance_of(Gender))
    gender_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterSex(IdentityEntityMixin, EnumEntity):
    sex: Sex = attr.ib(validator=attr.validators.instance_of(Sex))
    sex_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterRace(IdentityEntityMixin, EnumEntity):
    race: Race = attr.ib(validator=attr.validators.instance_of(Race))
    race_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterEthnicity(IdentityEntityMixin, EnumEntity):
    ethnicity: Ethnicity = attr.ib(validator=attr.validators.instance_of(Ethnicity))
    ethnicity_raw_text: str | None = attr.ib(default=None, validator=is_opt_str)
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterPhoneNumber(IdentityEntityMixin, Entity):
    number: str = attr.ib(validator=is_valid_phone_number)
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityClusterEmail(IdentityEntityMixin, Entity):
    address: str = attr.ib(validator=is_valid_email)
    identity_cluster: "IdentityCluster | None" = attr.ib(
        default=None, on_setattr=attr.setters.validate
    )


@attr.s(eq=False, kw_only=True, on_setattr=attr.setters.frozen)
class IdentityCluster(
    IdentityEntityMixin,
    HasMultipleExternalIdsEntity["IdentityClusterExternalId"],
    RootEntity,
):
    """The post-clustering view of one logical person."""

    external_ids: tuple["IdentityClusterExternalId", ...] = attr.ib(
        validator=is_tuple_of(IdentityClusterExternalId),
    )

    person_type: PersonType = attr.ib(validator=attr.validators.instance_of(PersonType))

    # Always None at runtime (enforced by is_none). Present because the manifest
    # compiler pairs every enum field with a _raw_text companion (see
    # EnumLiteralFieldManifest.additional_field_manifests). person_type comes
    # from $literal_enum in the YAML, which auto-injects None for the raw_text
    # counterpart. The type annotation stays `str | None` so the serialization
    # framework's attribute-type introspection keeps working.
    person_type_raw_text: str | None = attr.ib(default=None, validator=is_none)

    birthdate: datetime.date | None = attr.ib(
        default=None, validator=REASONABLE_OPT_BIRTHDATE_VALIDATOR
    )

    name: "IdentityClusterName | None" = attr.ib(
        default=None, validator=is_opt(IdentityClusterName)
    )

    gender: "IdentityClusterGender | None" = attr.ib(
        default=None, validator=is_opt(IdentityClusterGender)
    )

    sex: "IdentityClusterSex | None" = attr.ib(
        default=None, validator=is_opt(IdentityClusterSex)
    )

    races: tuple["IdentityClusterRace", ...] = attr.ib(
        factory=tuple,
        validator=is_tuple_of(IdentityClusterRace),
    )

    ethnicity: "IdentityClusterEthnicity | None" = attr.ib(
        default=None, validator=is_opt(IdentityClusterEthnicity)
    )

    phone_numbers: tuple["IdentityClusterPhoneNumber", ...] = attr.ib(
        factory=tuple,
        validator=is_tuple_of(IdentityClusterPhoneNumber),
    )

    emails: tuple["IdentityClusterEmail", ...] = attr.ib(
        factory=tuple,
        validator=is_tuple_of(IdentityClusterEmail),
    )

    # SHA-256 of the cluster's sorted external IDs; not a stable person
    # identifier. Full semantics documented in entity_field_descriptions.yaml.
    #
    # Declared with on_setattr=validate because its value is derived from
    # the fully-constructed, back-edge-resolved tree and so must be
    # assigned in __attrs_post_init__ rather than supplied by the caller.
    identity_cluster_id: str = attr.ib(
        init=False, default="", on_setattr=attr.setters.validate
    )

    # SHA-256 of the cluster's full content. Full semantics documented in
    # entity_field_descriptions.yaml.
    #
    # Declared with on_setattr=validate because its value is derived from
    # the fully-constructed, back-edge-resolved tree and so must be
    # assigned in __attrs_post_init__ rather than supplied by the caller.
    cluster_hash: str = attr.ib(
        init=False, default="", on_setattr=attr.setters.validate
    )

    def __attrs_post_init__(self) -> None:
        all_entities = get_all_entities_from_tree(
            self, IDENTITY_CLUSTER_ENTITIES_CONTEXT
        )
        mismatched_tenants = sorted(
            {
                e.tenant
                for e in all_entities
                if isinstance(e, IdentityEntityMixin) and e.tenant != self.tenant
            }
        )
        if mismatched_tenants:
            raise ValueError(
                f"All entities in the cluster must share the cluster's tenant "
                f"{self.tenant!r}; found mismatched tenants: {mismatched_tenants}"
            )

        set_backedges(self, IDENTITY_CLUSTER_ENTITIES_CONTEXT)

        json_entity_tree = serialize_entity_tree_into_json(
            self, IDENTITY_CLUSTER_ENTITIES_CONTEXT
        )
        json_entity_tree.pop("identity_cluster_id")
        json_entity_tree.pop("cluster_hash")

        # identity_cluster_id and cluster_hash are derived from the fully-
        # constructed tree (including back-edges, which `set_backedges` above
        # derives), so they can't be determined at construction time, but must
        # be computed here.
        self.identity_cluster_id = _hash_json(json_entity_tree["external_ids"])
        self.cluster_hash = _hash_json(json_entity_tree)

    def get_external_ids(self) -> list["IdentityClusterExternalId"]:
        # TODO(#84019): Loosen parent return type to Sequence and drop the
        # list(...) coercion here.
        return list(self.external_ids)

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "identity_cluster"


def _hash_json(payload: list[dict[str, Any]] | dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
