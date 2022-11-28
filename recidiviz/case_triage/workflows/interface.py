# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements common interface used to support external requests from Workflows."""
import base64
import json
import logging
import time
from http import HTTPStatus
from typing import Any, Dict

import requests
from flask import Response, jsonify, make_response

from recidiviz.utils.secrets import get_secret


class WorkflowsExternalRequestInterface:
    """Implements interface for Workflows-related external requests."""

    @staticmethod
    def insert_contact_note(data: Dict[str, Any]) -> Response:
        """
        Currently, this only implements the logic to send a test request to TOMIS.
        """

        def _handle_tomis_test_request() -> Response:
            """
            Used to make a test request to TOMIS with a provided fixture.
            """
            tomis_url = get_secret("workflows_us_tn_insert_contact_note_url")
            tomis_key = get_secret("workflows_us_tn_insert_contact_note_key")

            if tomis_url is None or tomis_key is None:
                return make_response(
                    jsonify(
                        message="Unable to get secrets for TOMIS",
                    ),
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            base64_encoded = base64.b64encode(tomis_key.encode())

            headers = {
                "Authorization": f"Basic {base64_encoded.decode()}",
                "Content-Type": "application/json",
            }

            tomis_request_fixture = data.get("fixture")
            if tomis_request_fixture is None:
                return make_response(
                    jsonify(
                        message="Does not have test fixture defined",
                    ),
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            timeout_secs = data.get("timeoutSecs", 5)
            tomis_request_body = json.dumps(tomis_request_fixture)
            start_time = time.perf_counter()
            try:
                logging.info("Sending request to TOMIS")
                tomis_response = requests.put(
                    tomis_url,
                    headers=headers,
                    data=tomis_request_body,
                    timeout=timeout_secs,
                )
                logging.info(
                    "Request to TOMIS completed in %s seconds",
                    round(time.perf_counter() - start_time, 2),
                )
                return make_response(
                    jsonify(
                        message="Called TOMIS",
                        data=tomis_response.json(),
                    ),
                    HTTPStatus.OK,
                )
            except Exception as e:
                logging.info(
                    "Request to TOMIS failed in %s seconds",
                    round(time.perf_counter() - start_time, 2),
                )
                return make_response(
                    jsonify(
                        message=f"An error occurred while calling TOMIS: {e}",
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    ),
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

        if data.get("isTest") and data.get("env") == "staging":
            return _handle_tomis_test_request()

        # Currently request should be made with specific shape for test purposes.
        # Return a bad request if it does not have necessary fields.
        return make_response(jsonify(), HTTPStatus.BAD_REQUEST)
