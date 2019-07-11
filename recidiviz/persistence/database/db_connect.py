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
Util for connecting the SQLAlchemy session to a given database and schema.
"""
import logging

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz
from recidiviz.persistence.database.jails_base_schema import JailsBase
from recidiviz.persistence.database.state_base_schema import StateBase
from recidiviz.utils import environment


def connect_session_to_database_instance(
        db_url: str, schema_base: DeclarativeMeta):
    engine = sqlalchemy.create_engine(db_url)
    schema_base.metadata.create_all(engine)
    recidiviz.Session.configure(bind=engine)
    _set_global_engine_for_schema(engine, schema_base)


def _set_global_engine_for_schema(engine: Engine,
                                  schema_base: DeclarativeMeta):
    if schema_base is JailsBase:
        recidiviz.jails_db_engine = engine
    elif schema_base is StateBase:
        recidiviz.state_db_engine = engine
    else:
        if environment.in_test():
            logging.info(
                "TEST-ONLY: Not setting global engine for schema base [%s]",
                schema_base.__name__)
            return
        raise ValueError(f"Unexpected DeclarativeMeta {schema_base.__name__}, "
                         f"unable to set engine.")
