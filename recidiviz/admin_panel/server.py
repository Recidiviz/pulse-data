# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""PoC server for admin panel"""
from http import HTTPStatus
from typing import Tuple

from flask import Flask

from recidiviz.utils import structured_logging
from recidiviz.utils.auth.gce import build_compute_engine_auth_decorator

structured_logging.setup()

app = Flask(__name__)


requires_authorization = build_compute_engine_auth_decorator(
    backend_service_id_secret_name="iap_admin_panel_load_balancer_service_id"  # nosec
)


@requires_authorization
@app.route("/health")
def health() -> Tuple[str, HTTPStatus]:
    """This just returns 200, and is used by Docker and GCP uptime checks to verify that the flask workers are
    up and serving requests."""
    return "", HTTPStatus.OK
