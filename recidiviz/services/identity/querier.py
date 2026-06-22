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
"""Data-access layer between the Identity Service Flask routes and the
Identity Postgres database. Methods return typed domain objects.
"""
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity.types import Identity


class IdentityServiceQuerier:
    """Implements Querier abstractions for the Identity Service data source."""

    @property
    def database_key(self) -> SQLAlchemyDatabaseKey:
        return SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)

    # TODO(#71776): Remove this example and its test when we add a real method.
    def get_all_identities(self) -> list[Identity]:
        """Returns every identity row in the database as a typed domain object."""
        with SessionFactory.using_database(self.database_key) as session:
            return [
                Identity(
                    recidiviz_id=row.recidiviz_id,
                    tenant=row.tenant,
                    person_type=row.person_type,
                    status=row.status,
                    merged_into=row.merged_into,
                    created_utc=row.created_utc,
                    last_updated_utc=row.last_updated_utc,
                )
                for row in session.query(schema.Identity).all()
            ]
