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
"""Config loader for the identity ingest pipeline."""
import os
from enum import Enum

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct import regions as regions_module
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.utils.yaml_dict import YAMLDict

_REGIONS_DIR = os.path.dirname(regions_module.__file__)
_IDENTITY_CONFIG_FILENAME = "identity_config.yaml"
_MAX_IDS_PER_TYPE_OVERRIDES_KEY = "max_ids_per_type_overrides"
_CONFLICT_CHECK_OVERRIDES_KEY = "conflict_check_overrides"
_RESOLUTION_STRATEGY_OVERRIDES_KEY = "resolution_strategy_overrides"


def identity_config_path_for_state_code(
    state_code: StateCode, regions_dir: str = _REGIONS_DIR
) -> str:
    """Returns the path to identity_config.yaml for the given state code."""
    return os.path.join(
        regions_dir, state_code.value.lower(), _IDENTITY_CONFIG_FILENAME
    )


class ResolutionStrategy(Enum):
    """How the pipeline resolves an attribute whose values diverge, but not
    enough to conflict, across a kept cluster's fragments. KEEP_LATEST takes the
    value from the fragment with the latest upper-bound date. SET_NULL stores
    nothing.
    """

    KEEP_LATEST = "keep_latest"
    SET_NULL = "set_null"


class ConflictCheckedAttribute(Enum):
    """A scalar IdentityAttributes field the pipeline checks for conflicts
    across a cluster's fragments, and whose benign divergence a tenant can set a
    resolution strategy for.
    """

    SURNAME = "surname"
    GIVEN_NAME = "given_name"
    MIDDLE_NAME = "middle_name"
    NAME_SUFFIX = "name_suffix"
    BIRTHDATE = "birthdate"
    SEX = "sex"
    GENDER = "gender"
    ETHNICITY = "ethnicity"


class OptionalConflictCheckedAttribute(Enum):
    """An attribute whose conflict-checking each tenant switches on or off.
    Some tenants' sources record the exact-match enums unreliably, so whether a
    mismatch on one rejects the cluster is a per-tenant choice. The name
    components, name suffix, and birthdate are always checked, so they are
    absent here.

    Members take their values from ConflictCheckedAttribute so the two enums
    cannot drift apart.
    """

    SEX = ConflictCheckedAttribute.SEX.value
    GENDER = ConflictCheckedAttribute.GENDER.value
    ETHNICITY = ConflictCheckedAttribute.ETHNICITY.value


ConflictCheckedAttributesConfig = dict[OptionalConflictCheckedAttribute, bool]
ResolutionStrategyConfig = dict[ConflictCheckedAttribute, ResolutionStrategy]


# Default maximum number of distinct fragments (source rows) that may carry a
# single external ID value of a given type, within one ingest view and snapshot
# date, before that value is treated as a sentinel (a placeholder like
# InmateNum="000000" shared across many unrelated people). This is a count of
# fragments sharing one value, not the number of IDs a single person may have.
# It is 1 because identity ingest views are authored one-row-per-person, so a
# real ID value lands on exactly one fragment while a sentinel lands on many.
# Tenants override specific id types via max_ids_per_type_overrides in their
# identity_config.yaml.
_DEFAULT_MAX_IDS_PER_TYPE = 1

# Code defaults; YAML holds only overrides. Sex is conflict-checked by default;
# gender and ethnicity are not.
_DEFAULT_CONFLICT_CHECKED_ATTRIBUTES_CONFIG: ConflictCheckedAttributesConfig = {
    OptionalConflictCheckedAttribute.SEX: True,
    OptionalConflictCheckedAttribute.GENDER: False,
    OptionalConflictCheckedAttribute.ETHNICITY: False,
}

# Code defaults; YAML holds only overrides. Names, name suffix, and birthdate
# keep the latest value on a benign divergence; the enums store nothing.
_DEFAULT_RESOLUTION_STRATEGY_CONFIG: ResolutionStrategyConfig = {
    ConflictCheckedAttribute.SURNAME: ResolutionStrategy.KEEP_LATEST,
    ConflictCheckedAttribute.GIVEN_NAME: ResolutionStrategy.KEEP_LATEST,
    ConflictCheckedAttribute.MIDDLE_NAME: ResolutionStrategy.KEEP_LATEST,
    ConflictCheckedAttribute.NAME_SUFFIX: ResolutionStrategy.KEEP_LATEST,
    ConflictCheckedAttribute.BIRTHDATE: ResolutionStrategy.KEEP_LATEST,
    ConflictCheckedAttribute.SEX: ResolutionStrategy.SET_NULL,
    ConflictCheckedAttribute.GENDER: ResolutionStrategy.SET_NULL,
    ConflictCheckedAttribute.ETHNICITY: ResolutionStrategy.SET_NULL,
}


@attr.define(frozen=True, kw_only=True)
class ConflictCheckedAttributeOverrides:
    """A tenant's conflict_check_overrides block, as written in its
    identity_config.yaml. Attributes absent here follow the code defaults.
    """

    overrides: ConflictCheckedAttributesConfig = attr.ib(
        validator=attr.validators.deep_mapping(
            key_validator=attr.validators.in_(OptionalConflictCheckedAttribute),
            value_validator=attr.validators.instance_of(bool),
            mapping_validator=attr.validators.instance_of(dict),
        )
    )
    """The optional attributes the tenant has explicitly switched on or off."""

    @classmethod
    def empty(cls) -> "ConflictCheckedAttributeOverrides":
        """Returns overrides that leave every attribute at its default."""
        return cls(overrides={})

    @classmethod
    def from_yaml_dict(
        cls, overrides_dict: YAMLDict
    ) -> "ConflictCheckedAttributeOverrides":
        """Parses a conflict_check_overrides block, rejecting any attribute whose
        conflict-checking is not configurable."""
        overrides: ConflictCheckedAttributesConfig = {}
        for attribute_key in list(overrides_dict.get()):
            try:
                attribute = OptionalConflictCheckedAttribute(attribute_key)
            except ValueError as e:
                raise ValueError(
                    f"Unexpected conflict_check_overrides attribute "
                    f"[{attribute_key}]; only "
                    f"{[a.value for a in OptionalConflictCheckedAttribute]} are "
                    f"configurable."
                ) from e
            overrides[attribute] = overrides_dict.pop(attribute_key, bool)
        return cls(overrides=overrides)

    def resolve(
        self, defaults: ConflictCheckedAttributesConfig
    ) -> ConflictCheckedAttributesConfig:
        """Returns the given defaults overlaid with these overrides. The result
        holds an entry for every OptionalConflictCheckedAttribute."""
        return {**defaults, **self.overrides}


@attr.define(frozen=True, kw_only=True)
class ResolutionStrategyOverrides:
    """A tenant's resolution_strategy_overrides block, as written in its
    identity_config.yaml. Attributes absent here follow the code defaults.
    """

    overrides: ResolutionStrategyConfig = attr.ib(
        validator=attr.validators.deep_mapping(
            key_validator=attr.validators.in_(ConflictCheckedAttribute),
            value_validator=attr.validators.in_(ResolutionStrategy),
            mapping_validator=attr.validators.instance_of(dict),
        )
    )
    """The attributes whose resolution strategy the tenant has explicitly set."""

    @classmethod
    def empty(cls) -> "ResolutionStrategyOverrides":
        """Returns overrides that leave every attribute at its default."""
        return cls(overrides={})

    @classmethod
    def from_yaml_dict(cls, overrides_dict: YAMLDict) -> "ResolutionStrategyOverrides":
        """Parses a resolution_strategy_overrides block, rejecting attributes that are not
        conflict-checked and strategies that are not recognized."""
        overrides: ResolutionStrategyConfig = {}
        for attribute_key in list(overrides_dict.get()):
            try:
                attribute = ConflictCheckedAttribute(attribute_key)
            except ValueError as e:
                raise ValueError(
                    f"Unexpected resolution_strategy_overrides attribute [{attribute_key}]; "
                    f"only {[a.value for a in ConflictCheckedAttribute]} are "
                    f"configurable."
                ) from e
            # pop(..., str) makes a YAML null (including a dangling "birthdate:")
            # fail loudly here rather than parse as a strategy.
            raw_strategy = overrides_dict.pop(attribute_key, str)
            try:
                overrides[attribute] = ResolutionStrategy(raw_strategy)
            except ValueError as e:
                raise ValueError(
                    f"Invalid resolution [{raw_strategy}] for [{attribute_key}]; "
                    f"expected one of {[s.value for s in ResolutionStrategy]}."
                ) from e
        return cls(overrides=overrides)

    def resolve(self, defaults: ResolutionStrategyConfig) -> ResolutionStrategyConfig:
        """Returns the given defaults overlaid with these overrides. The result
        holds an entry for every ConflictCheckedAttribute."""
        return {**defaults, **self.overrides}


@attr.define(frozen=True, kw_only=True)
class IdentityIngestPipelineTenantConfig:
    """Per-tenant, per-person-type configuration for the identity ingest
    pipeline. Each field stores a config block as written in YAML; the
    properties derive the effective values by overlaying the code defaults.
    """

    max_ids_per_type_overrides: dict[str, int] = attr.ib(
        factory=dict,
        validator=attr.validators.deep_mapping(
            key_validator=attr_validators.is_str,
            value_validator=attr_validators.is_int,
            mapping_validator=attr.validators.instance_of(dict),
        ),
    )
    """Per-id-type overrides of the sentinel threshold. ID types absent here
    fall back to _DEFAULT_MAX_IDS_PER_TYPE."""

    # Private so call sites read the effective conflict_checked_attributes_config
    # property rather than the raw overrides. attrs strips the leading underscore
    # for the __init__ keyword, so construct with conflict_check_overrides=...
    _conflict_check_overrides: ConflictCheckedAttributeOverrides = attr.ib(
        factory=ConflictCheckedAttributeOverrides.empty,
        validator=attr.validators.instance_of(ConflictCheckedAttributeOverrides),
    )
    """The tenant's conflict-check toggles, as written in YAML."""

    # Private so call sites read the effective resolution_strategy_config
    # property rather than the raw overrides. attrs strips the leading underscore
    # for the __init__ keyword, so construct with resolution_strategy_overrides=...
    _resolution_strategy_overrides: ResolutionStrategyOverrides = attr.ib(
        factory=ResolutionStrategyOverrides.empty,
        validator=attr.validators.instance_of(ResolutionStrategyOverrides),
    )
    """The tenant's resolution strategies, as written in YAML."""

    def get_max_ids_for_type(self, id_type: str) -> int:
        """Returns the sentinel threshold for the given id_type: its override
        from max_ids_per_type_overrides if set, else _DEFAULT_MAX_IDS_PER_TYPE.
        """
        return self.max_ids_per_type_overrides.get(id_type, _DEFAULT_MAX_IDS_PER_TYPE)

    @property
    def conflict_checked_attributes_config(self) -> ConflictCheckedAttributesConfig:
        """Returns the effective conflict-check toggles: the code defaults
        overlaid with this tenant's overrides. Holds an entry for every
        OptionalConflictCheckedAttribute."""
        return self._conflict_check_overrides.resolve(
            _DEFAULT_CONFLICT_CHECKED_ATTRIBUTES_CONFIG
        )

    @property
    def resolution_strategy_config(self) -> ResolutionStrategyConfig:
        """Returns the effective resolution strategies: the code defaults
        overlaid with this tenant's overrides. Holds an entry for every
        ConflictCheckedAttribute."""
        return self._resolution_strategy_overrides.resolve(
            _DEFAULT_RESOLUTION_STRATEGY_CONFIG
        )

    @classmethod
    def from_yaml_dict(
        cls, person_type_dict: YAMLDict
    ) -> "IdentityIngestPipelineTenantConfig":
        """Parses one person-type block of an identity_config.yaml."""
        max_ids_dict = person_type_dict.pop_dict_optional(
            _MAX_IDS_PER_TYPE_OVERRIDES_KEY
        )
        max_ids_per_type_overrides = (
            {
                id_type: max_ids_dict.pop(id_type, int)
                for id_type in list(max_ids_dict.get())
            }
            if max_ids_dict
            else {}
        )
        conflict_check_overrides = (
            ConflictCheckedAttributeOverrides.from_yaml_dict(d)
            if (d := person_type_dict.pop_dict_optional(_CONFLICT_CHECK_OVERRIDES_KEY))
            is not None
            else ConflictCheckedAttributeOverrides.empty()
        )
        resolution_strategy_overrides = (
            ResolutionStrategyOverrides.from_yaml_dict(d)
            if (
                d := person_type_dict.pop_dict_optional(
                    _RESOLUTION_STRATEGY_OVERRIDES_KEY
                )
            )
            is not None
            else ResolutionStrategyOverrides.empty()
        )
        if person_type_dict:
            raise ValueError(
                f"Found unexpected config values: {repr(person_type_dict.get())}"
            )
        return cls(
            max_ids_per_type_overrides=max_ids_per_type_overrides,
            conflict_check_overrides=conflict_check_overrides,
            resolution_strategy_overrides=resolution_strategy_overrides,
        )


@attr.define(frozen=True, kw_only=True)
class IdentityIngestPipelineConfig:
    """Top-level configuration for the identity ingest pipeline, parsed from
    YAML.
    """

    default_config: IdentityIngestPipelineTenantConfig
    """The config used for any (tenant, person_type) pair with no explicit
    block."""

    tenant_configs: dict[tuple[Tenant, PersonType], IdentityIngestPipelineTenantConfig]
    """The explicitly configured (tenant, person_type) blocks."""

    @classmethod
    def load_clustering_config(
        cls,
        regions_dir: str = _REGIONS_DIR,
    ) -> "IdentityIngestPipelineConfig":
        """Parses every state's identity_config.yaml into one typed config."""
        tenant_configs: dict[
            tuple[Tenant, PersonType], IdentityIngestPipelineTenantConfig
        ] = {}
        for state_code in get_direct_ingest_states_existing_in_env():
            tenant = Tenant.from_state_code(state_code)
            tenant_dict = YAMLDict.from_path(
                identity_config_path_for_state_code(state_code, regions_dir)
            )
            for person_type in PersonType:
                person_type_dict = tenant_dict.pop_dict_optional(
                    person_type.value.lower()
                )
                if person_type_dict is None:
                    continue
                try:
                    tenant_configs[
                        (tenant, person_type)
                    ] = IdentityIngestPipelineTenantConfig.from_yaml_dict(
                        person_type_dict
                    )
                except Exception as e:
                    raise ValueError(
                        f"Unable to parse identity config for [{state_code.value}] "
                        f"[{person_type.value}]: {e}"
                    ) from e

            if tenant_dict:
                raise ValueError(
                    f"Found unexpected top-level config values for identity "
                    f"config [{state_code.value}]: {repr(tenant_dict.get())}"
                )

        return cls(
            default_config=IdentityIngestPipelineTenantConfig(),
            tenant_configs=tenant_configs,
        )

    def get_tenant_clustering_config(
        self, tenant: Tenant, person_type: PersonType
    ) -> IdentityIngestPipelineTenantConfig:
        """Returns the config for the given tenant and person type, falling back
        to default_config for any pair that is not explicitly configured."""
        return self.tenant_configs.get((tenant, person_type), self.default_config)
