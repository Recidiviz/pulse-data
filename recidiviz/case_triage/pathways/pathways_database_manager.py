# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
""" Contains the database manager for Pathways """
from typing import Dict

from sqlalchemy.orm import sessionmaker

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.common.constants.states import _FakeStateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


class PathwaysDatabaseManager:
    """Class for managing Pathways database engine initialization / session factories"""

    pathways_session_factories: Dict[str, sessionmaker]

    def __init__(self) -> None:
        self.database_keys = {
            state_code: self.database_key_for_state(state_code)
            for state_code in get_pathways_enabled_states()
        }

        # Initialize engines. Silently no-ops if engines have already been initialized,
        # but also silently fails if the engine can not be created.
        SQLAlchemyEngineManager.attempt_init_engines_for_databases(
            list(self.database_keys.values())
        )

        self.pathways_session_factories = {
            state_code: sessionmaker(
                bind=SQLAlchemyEngineManager.get_engine_for_database(database_key)
            )
            for state_code, database_key in self.database_keys.items()
        }

    # TODO(#13950): Replace with StateCode
    def get_pathways_session(self, state_code: _FakeStateCode) -> sessionmaker:
        if state_code.value not in get_pathways_enabled_states():
            raise ValueError(f"StateCode {state_code} does not have Pathways enabled")

        return self.pathways_session_factories[state_code.value]

    @classmethod
    def database_key_for_state(cls, state_code: str) -> SQLAlchemyDatabaseKey:
        return SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, db_name=state_code.lower())
