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
from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.trace import config_integration
from opencensus.trace.exporters import file_exporter, stackdriver_exporter

from recidiviz.cloud_functions.cloud_functions import cloud_functions_blueprint
from recidiviz.ingest.aggregate.scrape_aggregate_reports import \
    scrape_aggregate_reports_blueprint
from recidiviz.ingest.scrape.infer_release import infer_release_blueprint
from recidiviz.ingest.scrape.scraper_control import scraper_control
from recidiviz.ingest.scrape.scraper_status import scraper_status
from recidiviz.ingest.scrape.worker import worker
from recidiviz.persistence.actions import actions
from recidiviz.persistence.batch_persistence import batch_blueprint
from recidiviz.tests.utils.populate_test_db import test_populator
from recidiviz.utils import environment, structured_logging, metadata

structured_logging.setup()

app = Flask(__name__)
app.register_blueprint(scraper_control, url_prefix='/scraper')
app.register_blueprint(scraper_status, url_prefix='/scraper')
app.register_blueprint(worker, url_prefix='/scraper')
app.register_blueprint(actions, url_prefix='/ingest')
app.register_blueprint(infer_release_blueprint, url_prefix='/infer_release')
app.register_blueprint(cloud_functions_blueprint, url_prefix='/cloud_function')
app.register_blueprint(batch_blueprint, url_prefix='/batch')
app.register_blueprint(
    scrape_aggregate_reports_blueprint, url_prefix='/scrape_aggregate_reports')
if not environment.in_gae():
    app.register_blueprint(test_populator, url_prefix='/test_populator')

# Setup tracing of requests not traced by default
if environment.in_gae():
    exporter = stackdriver_exporter.StackdriverExporter(
        project_id=metadata.project_id(), transport=AsyncTransport)
else:
    exporter = file_exporter.FileExporter(file_name='traces')
# TODO(596): This is a no-op until the next release of `opencensus`.
app.config['OPENCENSUS_TRACE_PARAMS'] = {
    'BLACKLIST_HOSTNAMES': ['metadata']  # Don't trace metadata requests
}
middleware = FlaskMiddleware(app, exporter=exporter)
config_integration.trace_integrations(
    ['google_cloud_clientlibs', 'requests', 'sqlalchemy'])
