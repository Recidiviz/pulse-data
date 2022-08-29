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
"""Defines enum for specifying an independent set of ingest data / infrastructure for a
given region.
"""
from enum import Enum

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class DirectIngestInstance(Enum):
    """Enum for specifying an independent set of ingest data / infrastructure for a
    given region.
    """

    # Ingest instance whose ingested data is exported to BQ and may be shipped to
    # products.
    PRIMARY = "PRIMARY"

    # Ingest instance that may be used for background ingest operations, such as a full
    # rerun.
    SECONDARY = "SECONDARY"

    def database_key_for_state(self, state_code: StateCode) -> SQLAlchemyDatabaseKey:
        """Returns the key to the database corresponding to the provided state code and
        database version.
        """
        db_name = f"{state_code.value.lower()}_{self.value.lower()}"

        return SQLAlchemyDatabaseKey(schema_type=SchemaType.STATE, db_name=db_name)
