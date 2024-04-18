#!/usr/bin/env bash

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
"""
File with helper functions used by recidiviz/case_triage/jii/id_lsu_routes.py and
recidiviz/case_triage/workflows/workflows_routes.py
"""
import logging
import re
from http import HTTPStatus
from typing import Callable, Optional

import werkzeug.wrappers
from flask import Response, make_response, request
from werkzeug.http import parse_set_header

from recidiviz.case_triage.workflows.twilio_validation import TwilioValidator
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.metadata import CloudRunMetadata

if in_gcp():
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server("case-triage-web")
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )

ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
    r"https\://app-staging\.recidiviz\.org$",
    cloud_run_metadata.url,
]

proxy_endpoint = "workflows.proxy"
webhook_endpoints = [
    "jii.handle_twilio_status",
    "jii.handle_twilio_incoming_message",
    "workflows.handle_twilio_status",
    "workflows.handle_twilio_incoming_message",
]

twilio_validator = TwilioValidator()


def validate_request_helper(
    handle_authorization: Callable, handle_recidiviz_only_authorization: Callable
) -> None:
    if request.method == "OPTIONS":
        return
    if request.endpoint in webhook_endpoints:
        logging.info("Twilio webhook endpoint request origin: [%s]", request.origin)

        signature = request.headers["X-Twilio-Signature"]
        params = request.values.to_dict()
        twilio_validator.validate(url=request.url, params=params, signature=signature)
        return
    if request.endpoint == proxy_endpoint:
        handle_recidiviz_only_authorization()
        return
    handle_authorization()


def validate_cors_helper() -> Optional[Response]:
    if request.endpoint in [proxy_endpoint] + webhook_endpoints:
        # Proxy or webhook requests will generally be sent from a developer's machine or Twilio server
        # and not a browser, so there is no origin to check against.
        return None

    is_allowed = any(
        re.match(allowed_origin, request.origin) for allowed_origin in ALLOWED_ORIGINS
    )

    if not is_allowed:
        response = make_response()
        response.status_code = HTTPStatus.FORBIDDEN
        return response

    return None


def add_cors_headers_helper(
    response: werkzeug.wrappers.Response,
) -> werkzeug.wrappers.Response:
    # Don't cache access control headers across origins
    response.vary = "Origin"
    response.access_control_allow_origin = request.origin
    response.access_control_allow_headers = parse_set_header(
        # `baggage` is added by sentry. It only seems to reach the server during local development
        "authorization, sentry-trace, x-csrf-token, content-type, baggage"
    )
    response.access_control_allow_credentials = True
    # Cache preflight responses for 2 hours
    response.access_control_max_age = 2 * 60 * 60
    return response
