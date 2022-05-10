# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Helpers for server setup."""
from typing import List

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


def database_keys_for_schema_type(
    schema_type: SchemaType,
) -> List[SQLAlchemyDatabaseKey]:
    """Returns a list of keys for **all** databases in the instance corresponding to
    this schema type.
    """
    if not schema_type.is_multi_db_schema:
        return [SQLAlchemyDatabaseKey.for_schema(schema_type)]

    if schema_type == SchemaType.STATE:
        return [
            ingest_instance.database_key_for_state(state_code)
            for ingest_instance in DirectIngestInstance
            for state_code in get_existing_direct_ingest_states()
        ]

    if schema_type == SchemaType.PATHWAYS:
        return [
            SQLAlchemyDatabaseKey(
                schema_type=SchemaType.PATHWAYS, db_name=state_code.lower()
            )
            for state_code in get_pathways_enabled_states()
        ]

    raise ValueError(f"Unexpected schema_type: [{schema_type}]")
