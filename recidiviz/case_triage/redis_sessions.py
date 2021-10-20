# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
""" Contains functionality for persisting sessions to Redis """
import logging
import pickle
from typing import Any, Dict, Optional
from uuid import uuid4

from flask import Flask, Request, Response
from flask.helpers import total_seconds
from flask.sessions import SessionInterface, SessionMixin
from itsdangerous import BadSignature, TimestampSigner
from redis import Redis
from werkzeug.datastructures import CallbackDict


class RedisSession(CallbackDict, SessionMixin):
    """ The RedisSession object is returned when referencing `flask.session` """

    session_id: str

    def __init__(
        self,
        session_id: str,
        data: Optional[Dict] = None,
    ) -> None:
        self.permanent = True
        self.modified = False
        self.session_id = session_id

        def on_update(_: Dict) -> None:
            self.modified = True

        super().__init__(data if data is not None else {}, on_update=on_update)

    def is_empty(self) -> bool:
        return not dict(self)


class SessionCannotBeFetchedError(ValueError):
    pass


class RedisSessionFactory:
    """ Contains functionality for storing sessions in Redis """

    def __init__(self, redis: Redis):
        self.redis = redis

    def fetch(self, session_id: str) -> RedisSession:
        session_cache_key = RedisSessionFactory.build_session_cache_key(session_id)
        session_data = self.redis.get(session_cache_key)

        if not session_data:
            raise SessionCannotBeFetchedError("Session with given session_id not found")

        try:
            data = pickle.loads(session_data)
        except pickle.PickleError as e:
            raise SessionCannotBeFetchedError("Session data could not be parsed") from e

        return RedisSession(session_id, data=data)

    def create_new_session(self) -> RedisSession:
        reserved_session_data = pickle.dumps({})

        while True:
            session_id = str(uuid4())
            session_cache_key = RedisSessionFactory.build_session_cache_key(session_id)

            # Reserve the cache key if it does not exist, then break
            if self.redis.setnx(session_cache_key, reserved_session_data):
                break

        return RedisSession(session_id=session_id)

    def remove_session(self, session_id: str) -> None:
        session_key = RedisSessionFactory.build_session_cache_key(session_id)
        self.redis.delete(session_key)

    @staticmethod
    def build_session_cache_key(session_id: str) -> str:
        return f"session-{session_id}"


class RedisSessionInterface(SessionInterface):
    """ Contains orchestration for storing sessions in Redis and sending session cookies"""

    pickle_based = True

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.session_factory = RedisSessionFactory(redis)

    def _build_cookie_options(
        self, app: Flask, session: RedisSession
    ) -> Dict[str, Any]:
        """ Builds the appropriate options for the session cookie, as dictated by our superclass """
        return {
            "expires": self.get_expiration_time(app, session),
            "httponly": self.get_cookie_httponly(app),
            "domain": self.get_cookie_domain(app),
            "path": self.get_cookie_path(app),
            "secure": self.get_cookie_secure(app),
            "samesite": self.get_cookie_samesite(app),
        }

    # The `types-flask` package uses an incorrect return signature for this interface method.
    # As documented in Flask, this method must return None if a session fails to load, otherwise it must return
    # an instance of a `SessionMixin` subclass that implements a dictionary-like interface.
    def open_session(self, app: Flask, request: Request) -> Optional[RedisSession]:  # type: ignore
        if not app.secret_key:
            raise ValueError("The Flask app must be configured with a secret_key")

        signed_session_id = request.cookies.get(app.session_cookie_name)
        session_id = None

        if signed_session_id:
            try:
                signer = TimestampSigner(app.secret_key)
                session_id = signer.unsign(
                    signed_session_id,
                    max_age=int(app.permanent_session_lifetime.total_seconds()),
                ).decode()
            # Catch exceptions when the signature doesn't match the provided `session_id`, or when signature has expired
            except BadSignature:
                return self.session_factory.create_new_session()

        if session_id:
            try:
                return self.session_factory.fetch(session_id)
            except SessionCannotBeFetchedError:
                logging.info("Cannot fetch session data for session %s", session_id)
                return self.session_factory.create_new_session()

        return self.session_factory.create_new_session()

    def save_session(
        self, app: Flask, session: RedisSession, response: Response
    ) -> None:
        """ Persists the session to Redis and sends the appropriate cookie headers to the client """
        if not app.secret_key:
            raise ValueError("The Flask app must be configured with a secret_key")

        signer = TimestampSigner(app.secret_key)

        # If the session has been cleared
        if session.modified and session.is_empty():
            # Remove the session from Redis
            self.session_factory.remove_session(session.session_id)

            # Clear the session cookie
            response.delete_cookie(
                app.session_cookie_name,
                domain=self.get_cookie_domain(app),
                path=self.get_cookie_path(app),
            )

            return

        # Store the session in Redis with an expiry
        self.redis.setex(
            name=RedisSessionFactory.build_session_cache_key(session.session_id),
            value=pickle.dumps(dict(session)),
            time=total_seconds(app.permanent_session_lifetime),
        )

        # Send the session id to the client
        response.set_cookie(
            app.session_cookie_name,
            signer.sign(session.session_id),
            **self._build_cookie_options(app, session),
        )
