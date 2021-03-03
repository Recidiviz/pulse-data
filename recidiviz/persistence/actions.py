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
"""Contains all HTTP endpoints for communicating with the persistence layer."""

from http import HTTPStatus
from typing import Tuple

from flask import Blueprint

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import persistence
from recidiviz.utils import monitoring
from recidiviz.utils.auth.gae import requires_gae_auth

actions = Blueprint("actions", __name__)


@actions.route("/v1/records", methods=["POST"])
@requires_gae_auth
def write_record() -> Tuple[str, HTTPStatus]:
    ingest_info = None
    last_scraped_time = None
    region = None
    jurisdiction_id = None

    with monitoring.push_tags({monitoring.TagKey.REGION: region}):
        metadata = IngestMetadata(region, jurisdiction_id, last_scraped_time)  # type: ignore

        persistence.write(ingest_info, metadata)  # type: ignore

        return "", HTTPStatus.NOT_IMPLEMENTED
