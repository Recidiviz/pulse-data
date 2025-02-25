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
"""Client interface for the asset generation service."""
import logging
from typing import Dict

import cattrs
import requests
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token
from requests import HTTPError

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.querier.querier import OutlierMetricInfo
from recidiviz.reporting.asset_generation.outliers_supervisor_chart import (
    OutliersSupervisorChartPayload,
    prepare_data,
)
from recidiviz.reporting.asset_generation.types import AssetResponseBase
from recidiviz.reporting.email_reporting_utils import get_env_var
from recidiviz.utils.environment import in_development, in_gcp


def _get_base_url() -> str:
    try:
        return get_env_var("ASSET_GENERATION_URL")
    except KeyError:
        if in_gcp() or in_development():
            raise

        return "http://asset-generation-test"


class AssetGenerationClient:
    """Client interface for the asset generation service."""

    _ASSET_GENERATION_SERVICE_URI = _get_base_url()

    # for local development we don't want an internal docker address,
    # because we don't request the files within a docker environment
    _RETRIEVE_API_BASE = (
        "http://localhost:5174" if in_development() else _ASSET_GENERATION_SERVICE_URI
    )

    def _auth_header(self) -> Dict[str, str]:
        if not in_gcp():
            return {}

        _id_token = fetch_id_token(
            request=Request(), audience=self._ASSET_GENERATION_SERVICE_URI
        )
        return {
            "Authorization": f"Bearer {_id_token}",
        }

    def _generate(self, endpoint: str, json: dict) -> requests.Response:
        resp = requests.post(
            f"{self._ASSET_GENERATION_SERVICE_URI}/generate/{endpoint}",
            json=json,
            headers=self._auth_header(),
            timeout=60,
        )
        try:
            resp.raise_for_status()
        except HTTPError:
            logging.error(resp.text, exc_info=True)
            raise

        return resp

    def generate_outliers_supervisor_chart(
        self, state_code: StateCode, asset_id: str, width: int, data: OutlierMetricInfo
    ) -> AssetResponseBase:
        payload = OutliersSupervisorChartPayload(
            stateCode=state_code.value,
            id=asset_id,
            width=width,
            data=prepare_data(data),
        )

        resp = self._generate("outliers-supervisor-chart", payload.to_dict())

        asset = cattrs.structure(resp.json(), AssetResponseBase)
        asset.url = f"{self._RETRIEVE_API_BASE}{asset.url}"

        return asset
