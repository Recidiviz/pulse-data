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
"""Request handlers for justice counts"""

from http import HTTPStatus
from typing import Tuple

from flask import Blueprint, request
from werkzeug import exceptions

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_value

justice_counts_control = Blueprint("justice_counts_control", __name__)


@justice_counts_control.route("/ingest")
@requires_gae_auth
def ingest() -> Tuple[str, HTTPStatus]:
    manifest_path = get_str_param_value(
        "manifest_path", request.args, preserve_case=True
    )

    if not manifest_path:
        raise exceptions.BadRequest("Parameter `manifest_path` is required.")

    manual_upload.ingest(
        GcsfsFactory.build(), GcsfsFilePath.from_absolute_path(manifest_path)
    )

    return "", HTTPStatus.OK
