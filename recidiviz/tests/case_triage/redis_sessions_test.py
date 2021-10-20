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
"""Implements tests to enforce that redis sessions work."""
from collections import namedtuple
from datetime import datetime
from http.cookies import SimpleCookie
from unittest import TestCase
from uuid import uuid4

import freezegun
from fakeredis import FakeRedis
from flask import Flask, Response, jsonify, session
from flask.sessions import SecureCookieSessionInterface
from itsdangerous import base64_decode
from itsdangerous.encoding import bytes_to_int

from recidiviz.case_triage.redis_sessions import (
    RedisSessionFactory,
    RedisSessionInterface,
)

test_app = Flask(__name__)
test_app.secret_key = str(uuid4())
fake_redis = FakeRedis()
test_app.session_interface = RedisSessionInterface(fake_redis)


@test_app.route("/modify_session")
def modify_session() -> Response:
    session["last_modified_at"] = datetime.now().isoformat()
    return jsonify(dict(session))


@test_app.route("/clear_session")
def clear_session() -> Response:
    session.clear()
    return jsonify(dict(session))


SessionCookieParts = namedtuple(
    "SessionCookieParts", ["session_id", "timestamp", "signature"]
)


def decode_signed_timestamp(timestamp: str) -> datetime:
    return datetime.fromtimestamp(bytes_to_int(base64_decode(timestamp)))


class TestRedisSessionInterface(TestCase):
    """Implements tests to enforce that demo users work."""

    def setUp(self) -> None:
        self.test_client = test_app.test_client()

    def tearDown(self) -> None:
        fake_redis.flushdb()

    def parse_session_cookie(self) -> SessionCookieParts:
        session_cookie = None

        for cookie in self.test_client.cookie_jar:
            if cookie.name == test_app.session_cookie_name:
                session_cookie = cookie
                break

        if not session_cookie:
            raise KeyError("Could not find session cookie")

        return SessionCookieParts(*session_cookie.value.split("."))

    def modify_session(self) -> SessionCookieParts:
        self.test_client.get("/modify_session")
        return self.parse_session_cookie()

    def test_no_session_cookie(self) -> None:
        """ When no session cookie is provided, the server assigns a session to the user"""
        first_session_cookie = self.modify_session()

        # The session persists across requests
        second_session_cookie = self.modify_session()

        self.assertEqual(
            first_session_cookie.session_id, second_session_cookie.session_id
        )

    def test_session_cookie_expiration(self) -> None:
        """ When an expired session cookie is provided, the server re-assigns a new session to the user"""
        first_session_cookie = self.modify_session()
        cache_key = RedisSessionFactory.build_session_cache_key(
            first_session_cookie.session_id
        )

        # Simulate session expiry
        fake_redis.delete(cache_key)

        second_session_cookie = self.modify_session()
        self.assertNotEqual(
            first_session_cookie.session_id, second_session_cookie.session_id
        )

    def test_expired_cookie_replay(self) -> None:
        """ If an expired cookie is sent to the server, the server issues the client a new session"""
        with freezegun.freeze_time(datetime(year=2021, month=1, day=1)):
            first_session_cookie = self.modify_session()

        with freezegun.freeze_time(datetime(year=2022, month=1, day=1)):
            second_session_cookie = self.modify_session()

        self.assertNotEqual(
            first_session_cookie.session_id, second_session_cookie.session_id
        )

    def test_session_clear_removes_session_data(self) -> None:
        self.assertEqual(fake_redis.keys(), [])
        first_session_cookie = self.modify_session()
        self.assertEqual(
            fake_redis.keys(),
            [
                RedisSessionFactory.build_session_cache_key(
                    first_session_cookie.session_id
                ).encode("utf-8")
            ],
        )

        clear_response = self.test_client.get("/clear_session")

        # Redis session data is cleared when the session is cleared
        self.assertEqual(fake_redis.keys(), [])

        # The session cookie is cleared when the session is cleared
        cookie: SimpleCookie = SimpleCookie()
        cookie.load(clear_response.headers["Set-Cookie"])
        session_cookie = cookie[test_app.session_cookie_name]

        self.assertIsNotNone(session_cookie)
        self.assertEqual(session_cookie["max-age"], "0")
        self.assertEqual(session_cookie["expires"], "Thu, 01-Jan-1970 00:00:00 GMT")
        self.assertEqual(session_cookie.value, "")

        # A new session is assigned on a subsequent request
        next_session_cookie = self.modify_session()
        self.assertNotEqual(
            first_session_cookie.session_id, next_session_cookie.session_id
        )

    def test_transition_from_cookie_session_to_redis_session(self) -> None:
        """ When a legacy client sends a cookie-based session, it is dropped, and a new redis-based session is sent """
        legacy_interface = SecureCookieSessionInterface()
        legacy_signer = legacy_interface.get_signing_serializer(test_app)
        with freezegun.freeze_time(datetime(2021, 10, 1)):
            cookie_session_contents = legacy_signer.dumps(
                dict({"last_modified_at": datetime.now()})
            )
            cookie_session_parts = SessionCookieParts(
                *cookie_session_contents.split(".")
            )

            self.test_client.set_cookie(
                "localhost",
                test_app.session_cookie_name,
                cookie_session_contents,
            )

        with freezegun.freeze_time(datetime(2021, 10, 2)):
            redis_session_cookie = self.modify_session()

        self.assertNotEqual(
            cookie_session_parts.session_id, redis_session_cookie.session_id
        )
        self.assertNotEqual(
            cookie_session_parts.signature, redis_session_cookie.signature
        )
        self.assertNotEqual(
            cookie_session_parts.timestamp, redis_session_cookie.timestamp
        )
