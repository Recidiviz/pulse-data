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
"""Implements helpers for setting up scoped db sessions for a Flask app.

This helper is also useful when setting up test flask apps for testing.
"""
from flask import Flask
from flask_sqlalchemy_session import flask_scoped_session
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def setup_scoped_sessions(app: Flask, db_url: str) -> Engine:
    engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
        database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE),
        db_url=db_url,
    )
    session_factory = sessionmaker(bind=engine)
    app.scoped_session = flask_scoped_session(session_factory, app)
    return engine
