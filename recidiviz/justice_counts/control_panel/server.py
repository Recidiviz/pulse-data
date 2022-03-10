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
"""Backend entry point for Justice Counts Control Panel backend API."""
from flask import Flask


# Note that we don't have to explicitly call `create_app` anywhere;
# Flask will automatically detect the factory function during `flask run`
# source: https://flask.palletsprojects.com/en/2.0.x/patterns/appfactories/#using-applications
# TODO(#11503): Add automatic endpoint documentation generation
def create_app() -> Flask:
    app = Flask(__name__)

    # TODO(#11504): Replace dummy endpoint
    @app.route("/hello")
    def hello() -> str:
        return "Hello, World!"

    return app
