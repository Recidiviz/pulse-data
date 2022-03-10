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
"""Implements helpers for working with SQLAlchemy in a Flask app."""
from flask import Flask
from flask_sqlalchemy_session import flask_scoped_session
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import sessionmaker

from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def setup_scoped_sessions(
    app: Flask, database_key: SQLAlchemyDatabaseKey, db_url: URL
) -> Engine:
    engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
        database_key=database_key,
        db_url=db_url,
    )
    session_factory = sessionmaker(bind=engine)
    flask_scoped_session(session_factory, app)
    return engine
