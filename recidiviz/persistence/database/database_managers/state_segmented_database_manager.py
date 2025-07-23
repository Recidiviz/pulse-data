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
""" Contains the database manager class for databases that are segmented by state """
from typing import Dict, List

from sqlalchemy.orm import sessionmaker

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


class StateSegmentedDatabaseManager:
    """
    Class for managing database engine initialization / session factories for databases
    that have a multi-database schema by state
    """

    session_factories: Dict[str, sessionmaker]

    def __init__(
        self,
        enabled_states: List[str],
        schema_type: SchemaType,
        using_proxy: bool = False,
    ) -> None:
        self.enabled_states = enabled_states
        self.schema_type = schema_type

        self.database_keys = {
            state_code: self.database_key_for_state(state_code)
            for state_code in self.enabled_states
        }

        # Initialize engines. Silently no-ops if engines have already been initialized,
        # but also silently fails if the engine can not be created.
        if not using_proxy:
            SQLAlchemyEngineManager.attempt_init_engines_for_databases(
                list(self.database_keys.values())
            )

        self.session_factories = {
            state_code: sessionmaker(
                bind=SQLAlchemyEngineManager.get_engine_for_database(database_key)
                if not using_proxy
                else SQLAlchemyEngineManager.get_engine_for_database_with_proxy(
                    database_key=database_key
                )
            )
            for state_code, database_key in self.database_keys.items()
        }

    def get_session(self, state_code: StateCode) -> sessionmaker:
        if state_code.value not in self.enabled_states:
            raise ValueError(f"StateCode {state_code} does not have a database enabled")

        return self.session_factories[state_code.value]

    def database_key_for_state(self, state_code: str) -> SQLAlchemyDatabaseKey:
        return SQLAlchemyDatabaseKey(self.schema_type, db_name=state_code.lower())
