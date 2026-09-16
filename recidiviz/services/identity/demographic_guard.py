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
"""Demographic guard for the Identity Service import: decides whether an
import operation may change an identity's attributes."""
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)


class DemographicGuard:
    """Demographic guard for the Identity Service import: decides whether an
    import operation may change an identity's attributes."""

    def allows_update(
        self, *, identity: schema.Identity, cluster: IdentityCluster
    ) -> bool:
        """Returns whether the cluster's values may be applied to the identity."""
        # TODO(OBT-37727): Compare the identity's EXTERNAL_DATA_SYSTEM
        # attributes against the cluster's (last names, dates of birth, gender,
        # race, emails) per the tenant's per-person-type config, allowing
        # everything when identity.skip_demographic_guard is set.
        del identity, cluster
        return True
