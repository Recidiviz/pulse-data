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
"""Implements tests for the Justice Counts Control Panel backend API."""
from http import HTTPStatus

import pytest
from sqlalchemy.engine import Engine

from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.persistence.database.schema.justice_counts.schema import Source
from recidiviz.tests.auth.utils import get_test_auth0_config
from recidiviz.tests.justice_counts.utils import JusticeCountsDatabaseTestCase
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.auth.auth0 import passthrough_authorization_decorator


@pytest.mark.uses_db
class TestJusticeCountsControlPanelAPI(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel backend API."""

    def setUp(self) -> None:
        test_config = Config(
            DB_URL=local_postgres_helpers.on_disk_postgres_db_url(),
            WTF_CSRF_ENABLED=False,
            AUTH_DECORATOR=passthrough_authorization_decorator(),
            AUTH0_CONFIGURATION=get_test_auth0_config(),
        )
        self.app = create_app(config=test_config)
        self.client = self.app.test_client()
        # `flask_sqlalchemy_session` sets the `scoped_session` attribute on the app,
        # even though this is not specified in the types for `app`.
        self.session = self.app.scoped_session  # type: ignore[attr-defined]

        super().setUp()

    def get_engine(self) -> Engine:
        return self.session.get_bind()

    def test_hello(self) -> None:
        response = self.client.get("/api/hello")
        self.assertEqual(response.data, b"Hello, World!")

    def test_logout(self) -> None:
        # TODO(#11550): Test logout endpoint
        response = self.client.post("/auth/logout")
        self.assertEqual(response.status_code, HTTPStatus.OK)

    def test_session(self) -> None:
        # Add data
        name = "Agency Alpha"
        self.session.add(Source(name=name))
        self.session.commit()

        # Query data
        source = self.session.query(Source).one_or_none()

        self.assertEqual(source.name, name)
