# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Entrypoint for the application."""

from flask import Flask

from recidiviz.ingest.scraper_control import scraper_control
from recidiviz.ingest.worker import worker
from recidiviz.tests.utils.populate_test_db import test_populator
from recidiviz.utils import environment

app = Flask(__name__)
app.register_blueprint(scraper_control, url_prefix='/scraper')
app.register_blueprint(worker, url_prefix='/scraper')
if not environment.in_prod():
    app.register_blueprint(test_populator, url_prefix='/test_populator')
