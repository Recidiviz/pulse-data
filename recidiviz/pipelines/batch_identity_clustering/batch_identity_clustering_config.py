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
"""Config loader for the batch identity clustering pipeline."""
import glob
import os

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.utils.yaml_dict import YAMLDict

_REGIONS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "ingest", "direct", "regions"
)
_IDENTITY_CONFIG_FILENAME = "identity_config.yaml"
_MAX_IDS_PER_TYPE_OVERRIDES_KEY = "max_ids_per_type_overrides"

# Default maximum number of external IDs of a given type a person can have before
# that ID value is assumed to be a sentinel (e.g. InmateNum='000000'). States can
# override this per-id-type in their identity_config.yaml.
_DEFAULT_MAX_IDS_PER_TYPE = 1


@attr.define(frozen=True)
class BatchIdentityClusteringTenantConfig:
    """Per-tenant, per-person-type configuration for the batch identity clustering pipeline."""

    # Overrides for the maximum number of external IDs of a given type a person can
    # have before that ID value is assumed to be a sentinel with no real ID meaning.
    # For example, a booking number might legitimately appear 50+ times for someone
    # who has been incarcerated many times, while a state ID should only appear once.
    # ID types not listed here fall back to _DEFAULT_MAX_IDS_PER_TYPE.
    max_ids_per_type_overrides: dict[str, int] = attr.Factory(dict)

    def get_max_ids_for_type(self, id_type: str) -> int:
        """Returns the maximum number of IDs of the given type a person can have
        before the ID value is assumed to be a sentinel. Defaults to
        _DEFAULT_MAX_IDS_PER_TYPE (currently 1)."""
        return self.max_ids_per_type_overrides.get(id_type, _DEFAULT_MAX_IDS_PER_TYPE)


@attr.define(frozen=True)
class BatchIdentityClusteringConfig:
    """Top-level configuration for the batch identity clustering pipeline, parsed from YAML."""

    default_config: BatchIdentityClusteringTenantConfig
    tenant_configs: dict[tuple[str, PersonType], BatchIdentityClusteringTenantConfig]

    @classmethod
    def load_clustering_config(
        cls,
        regions_dir: str = _REGIONS_DIR,
    ) -> "BatchIdentityClusteringConfig":
        """Parses the batch identity clustering config into a typed config object."""
        default_config = BatchIdentityClusteringTenantConfig()

        tenant_configs: dict[
            tuple[str, PersonType], BatchIdentityClusteringTenantConfig
        ] = {}
        for identity_config_path in glob.glob(
            os.path.join(regions_dir, "*", _IDENTITY_CONFIG_FILENAME)
        ):
            tenant = os.path.basename(os.path.dirname(identity_config_path)).upper()
            tenant_dict = YAMLDict.from_path(identity_config_path)
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
                    tenant_configs[
                        (tenant, person_type)
                    ] = BatchIdentityClusteringTenantConfig(
                        max_ids_per_type_overrides=overrides
                    )

        return cls(default_config=default_config, tenant_configs=tenant_configs)

    def get_tenant_clustering_config(
        self, tenant: str, person_type: PersonType
    ) -> BatchIdentityClusteringTenantConfig:
        """Returns the config for the given tenant and person type.

        Falls back to default_config for any (tenant, person_type) pair not explicitly
        configured. Tenant matching is case-insensitive.
        """
        return self.tenant_configs.get(
            (tenant.upper(), person_type), self.default_config
        )
