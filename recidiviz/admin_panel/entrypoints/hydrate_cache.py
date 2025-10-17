# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
# To refresh data in the admin panel cache
docker compose -f docker-compose.yaml -f docker-compose.admin-panel.yaml run admin_panel_cache_hydration
"""
import logging

from recidiviz.admin_panel.admin_stores import initialize_admin_stores
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.server_config import initialize_engines
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import set_development_project_id_override

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if in_development():
        set_development_project_id_override(GCP_PROJECT_STAGING)

    stores = initialize_admin_stores()

    initialize_engines(schema_types=[SchemaType.OPERATIONS])

    exceptions = []
    for store in stores.all_stores:
        logging.info("Hydrating %s", store)
        try:
            store.hydrate_cache()
        except Exception as e:
            logging.exception("Failed to hydrate cache for %s: %s", store, e)
            exceptions.append(e)

    if exceptions:
        raise ExceptionGroup(
            f"Failed to hydrate {len(exceptions)} store(s). See logs for details.",
            exceptions,
        )
