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
""" Endpoints for the data discovery tool. Combination of cloud task endpoints and user-facing endpoints
    Cloud task endpoints are suffixed with `_task`
"""

import attr
from flask import Blueprint, request, jsonify, Response

from recidiviz.admin_panel.data_discovery.file_configs import (
    get_raw_data_configs,
    get_ingest_view_configs,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
)
from recidiviz.utils.auth.gae import requires_gae_auth


def add_data_discovery_routes(blueprint: Blueprint) -> None:
    """ Adds data discovery routes to the passed Flask Blueprint"""

    @blueprint.route("/data_discovery/files", methods=["POST"])
    @requires_gae_auth
    def _files() -> Response:
        """Endpoint responsible for sending raw file / ingest view configs to the front-end

        Example:
            POST /admin/data_discovery/files
        Request Body:
            region_code: (string) The region code you wish to receive file configs for
        Returns:
            JSON representation of `DataDiscoveryStandardizedFileConfig`s for the region
        """
        return jsonify(
            {
                GcsfsDirectIngestFileType.RAW_DATA.value: {
                    config.file_tag: attr.asdict(config)
                    for config in get_raw_data_configs(request.json["region_code"])
                },
                GcsfsDirectIngestFileType.INGEST_VIEW.value: {
                    config.file_tag: attr.asdict(config)
                    for config in get_ingest_view_configs(request.json["region_code"])
                },
            }
        )
