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
from contextlib import contextmanager
from typing import Iterator, Optional

from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils import environment


class SessionFactory:
    """Creates SQLAlchemy sessions for the given database schema"""

    @classmethod
    @contextmanager
    def using_database(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        *,
        autocommit: bool = True,
        secret_prefix_override: Optional[str] = None,
    ) -> Iterator[Session]:
        """Implements a context manager for db sessions for |database_key|."""
        session = None
        try:
            engine = SQLAlchemyEngineManager.get_engine_for_database(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )
            if engine is None:
                raise ValueError(f"No engine set for key [{database_key}]")

            session = Session(bind=engine)
            cls._alter_session_variables(session)
            yield session
            if autocommit:
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    raise e
        finally:
            if session:
                session.close()

    @classmethod
    @contextmanager
    @environment.local_only
    def for_proxy(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        *,
        autocommit: bool = True,
        secret_prefix_override: Optional[str] = None,
    ) -> Iterator[Session]:
        """Implements a context manager for db sessions for use with the Cloud SQL Proxy."""
        engine = SQLAlchemyEngineManager.get_engine_for_database_with_proxy(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
        if engine is None:
            raise ValueError(f"No engine set for key [{database_key}]")

        session = None
        try:
            session = Session(bind=engine)
            cls._alter_session_variables(session)
            yield session
            if autocommit:
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    raise e
        finally:
            if session:
                session.close()

    @classmethod
    def _alter_session_variables(cls, session: Session) -> None:
        # Postgres uses a query cost analysis heuristic to decide what type of read to use for a particular query. It
        # sometimes chooses to use a sequential read because for hard disk drives (HDDs, as opposed to solid state
        # drives, SSDs) that may be faster than jumping around to random pages of an index. This is especially likely
        # when running over small sets of data. Setting this option changes the heuristic to almost always prefer index
        # reads.
        #
        # Our postgres instances run on SSDs, so this should increase performance for us. This is also important
        # because sequential reads lock an entire table, whereas index reads only lock the particular predicate from a
        # query. See https://www.postgresql.org/docs/12/transaction-iso.html and
        # https://stackoverflow.com/questions/42288808/why-does-postgresql-serializable-transaction-think-this-as-conflict.
        #
        # TODO(#3928): Once defined in code, set this on the SQL instance itself instead of per session.
        if session.bind.dialect.name == "postgresql":
            session.execute("SET random_page_cost=1;")
