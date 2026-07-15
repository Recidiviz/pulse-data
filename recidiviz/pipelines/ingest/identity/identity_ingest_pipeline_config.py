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

import attr

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


def identity_config_path_for_state_code(
    state_code: StateCode, regions_dir: str = _REGIONS_DIR
) -> str:
    """Returns the path to `identity_config.yaml` for the given state code."""
    return os.path.join(
        regions_dir, state_code.value.lower(), _IDENTITY_CONFIG_FILENAME
    )


# Default maximum number of distinct fragments (source rows) that may carry a
# single external ID value of a given type, within one ingest view and snapshot
# date, before that value is treated as a sentinel (a placeholder like
# InmateNum='000000' shared across many unrelated people). This is a count of
# fragments sharing one value, NOT the number of IDs a single person may have.
# It is 1 because identity ingest views are authored one-row-per-person, so a
# real ID value lands on exactly one fragment while a sentinel lands on many.
# Tenants can override specific id types via `max_ids_per_type_overrides` in their
# identity_config.yaml.
_DEFAULT_MAX_IDS_PER_TYPE = 1


@attr.define(frozen=True)
class IdentityIngestPipelineTenantConfig:
    """Per-tenant, per-person-type configuration for the identity ingest pipeline."""

    # Per-id-type overrides of the sentinel threshold. ID types not listed here
    # fall back to _DEFAULT_MAX_IDS_PER_TYPE (see there for what the threshold
    # means).
    max_ids_per_type_overrides: dict[str, int] = attr.Factory(dict)

    def get_max_ids_for_type(self, id_type: str) -> int:
        """Returns the sentinel threshold for the given id_type: its override
        from `max_ids_per_type_overrides` if set, else _DEFAULT_MAX_IDS_PER_TYPE."""
        return self.max_ids_per_type_overrides.get(id_type, _DEFAULT_MAX_IDS_PER_TYPE)


@attr.define(frozen=True)
class IdentityIngestPipelineConfig:
    """Top-level configuration for the identity ingest pipeline, parsed from YAML."""

    default_config: IdentityIngestPipelineTenantConfig
    tenant_configs: dict[tuple[Tenant, PersonType], IdentityIngestPipelineTenantConfig]

    @classmethod
    def load_clustering_config(
        cls,
        regions_dir: str = _REGIONS_DIR,
    ) -> "IdentityIngestPipelineConfig":
        """Parses the identity ingest pipeline config into a typed config object."""
        default_config = IdentityIngestPipelineTenantConfig()

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
                if person_type_dict is not None:
                    overrides_dict = person_type_dict.pop_dict_optional(
                        _MAX_IDS_PER_TYPE_OVERRIDES_KEY
                    )
                    overrides: dict[str, int] = (
                        {
                            id_type: overrides_dict.pop(id_type, int)
                            for id_type in list(overrides_dict.get())
                        }
                        if overrides_dict
                        else {}
                    )
                    if person_type_dict:
                        raise ValueError(
                            f"Found unexpected config values for identity config "
                            f"[{state_code.value}] [{person_type.value}]: "
                            f"{repr(person_type_dict.get())}"
                        )
                    tenant_configs[
                        (tenant, person_type)
                    ] = IdentityIngestPipelineTenantConfig(
                        max_ids_per_type_overrides=overrides,
                    )

            if tenant_dict:
                raise ValueError(
                    f"Found unexpected top-level config values for identity "
                    f"config [{state_code.value}]: {repr(tenant_dict.get())}"
                )

        return cls(default_config=default_config, tenant_configs=tenant_configs)

    def get_tenant_clustering_config(
        self, tenant: Tenant, person_type: PersonType
    ) -> IdentityIngestPipelineTenantConfig:
        """Returns the config for the given tenant and person type.

        Falls back to default_config for any (tenant, person_type) pair not explicitly
        configured.
        """
        return self.tenant_configs.get((tenant, person_type), self.default_config)
