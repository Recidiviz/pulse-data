# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
Class for generating SQLAlchemy Sessions objects for the appropriate schema.
"""

from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.base_schema import OperationsBase
from recidiviz.persistence.database.schema.operations.session_listener import (
    session_listener as operations_session_listener
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import \
    SQLAlchemyEngineManager
from recidiviz.persistence.database.session import Session


class SessionFactory:
    @classmethod
    def for_schema_base(cls, schema_base: DeclarativeMeta) -> Session:
        engine = SQLAlchemyEngineManager.get_engine_for_schema_base(schema_base)
        if engine is None:
            raise ValueError(f"No engine set for base [{schema_base.__name__}]")

        session = Session(bind=engine)
        cls._apply_session_listener_for_schema_base(schema_base, session)
        return session

    @classmethod
    def _apply_session_listener_for_schema_base(cls, schema_base: DeclarativeMeta, session):
        if schema_base == OperationsBase:
            operations_session_listener(session)
