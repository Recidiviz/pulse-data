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
from typing import Any, Callable, Optional

from flask import Flask, current_app, has_app_context
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from werkzeug.local import LocalProxy

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def setup_scoped_sessions(
    app: Flask,
    schema_type: SchemaType,
    database_url_override: Optional[str] = None,
    secret_prefix_override: Optional[str] = None,
) -> Engine:
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    database_url = (
        database_url_override
        or SQLAlchemyEngineManager.get_server_postgres_instance_url(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )
    )

    engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
        database_key=database_key,
        db_url=database_url,
    )

    flask_scoped_session(sessionmaker(bind=engine), app)

    return engine


def _get_session() -> Session:
    # pylint: disable=missing-docstring, protected-access
    if not has_app_context():
        raise RuntimeError(
            "Cannot access current_session when outside of an application context."
        )
    app = current_app
    if not hasattr(app, "scoped_session"):
        raise AttributeError(
            f"{app} has no 'scoped_session' attribute. You need to initialize it with a flask_scoped_session."
        )
    return app.scoped_session


current_session: Session = LocalProxy(_get_session)

# Scope the session to the current greenlet if greenlet is available,
# otherwise fall back to the current thread.
try:
    from greenlet import getcurrent as _ident_func
except ImportError:
    from threading import get_ident as _ident_func


class flask_scoped_session(scoped_session):
    """A :class:`~sqlalchemy.orm.scoping.scoped_session` whose scope is set to
    the Flask application context.
    """

    def __init__(
        self, session_factory: Callable[[], Session], app: Flask | None = None
    ) -> None:
        """
        :param session_factory: A callable that returns a
            :class:`~sqlalchemy.orm.session.Session`
        :param app: a :class:`~flask.Flask` application
        """
        super().__init__(session_factory, scopefunc=_ident_func)

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        """Setup scoped session creation and teardown for the passed ``app``.

        :param app: a :class:`~flask.Flask` application
        """
        app.scoped_session = self  # type: ignore

        @app.teardown_appcontext
        def remove_scoped_session(*args: Any, **kwargs: Any) -> None:
            # pylint: disable=missing-docstring,unused-argument,unused-variable
            if not hasattr(app, "scoped_session") or not isinstance(
                app.scoped_session, scoped_session
            ):

                raise AttributeError(
                    f"{app} has not been initialized with a flask_scoped_session"
                )

            app.scoped_session.remove()
