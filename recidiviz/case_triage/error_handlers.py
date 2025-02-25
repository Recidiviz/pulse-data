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
""" Contains error handlers to our Flask app"""
from http import HTTPStatus

from flask import Flask, Response, jsonify
from flask_wtf.csrf import CSRFError
from jwt import MissingRequiredClaimError
from marshmallow import ValidationError

from recidiviz.case_triage.pathways.exceptions import (
    MetricMappingError,
    MetricNotEnabledError,
)
from recidiviz.utils.flask_exception import FlaskException


def handle_auth_error(ex: FlaskException) -> Response:
    response = jsonify(
        {
            "code": ex.code,
            "description": ex.description,
        }
    )
    response.status_code = ex.status_code
    return response


def handle_validation_error(ex: ValidationError) -> Response:
    return handle_auth_error(
        FlaskException(
            code="bad_request",
            description=ex.messages,
            status_code=HTTPStatus.BAD_REQUEST,
        )
    )


def handle_csrf_error(error: CSRFError) -> Response:
    return handle_auth_error(
        FlaskException(
            code="invalid_csrf_token",
            description=f"The provided X-CSRF-Token header could not be validated ({error.description})",
            status_code=HTTPStatus.BAD_REQUEST,
        )
    )


def handle_missing_required_claim_error(error: MissingRequiredClaimError) -> Response:
    return handle_auth_error(
        FlaskException(
            code="missing_required_claim",
            description=f"{error.claim} was missing from the provided token",
            status_code=HTTPStatus.BAD_REQUEST,
        )
    )


def handle_metric_mapping_error(error: MetricMappingError) -> Response:
    return handle_auth_error(
        FlaskException(
            code="metric_mapping_error",
            description=error.message,
            status_code=HTTPStatus.BAD_REQUEST,
        )
    )


def handle_metric_not_enabled_error(error: MetricNotEnabledError) -> Response:
    return handle_auth_error(
        FlaskException(
            code="metric_not_enabled",
            description=f"{error.metric_name} is not enabled for {error.state_code.value}",
            status_code=HTTPStatus.BAD_REQUEST,
        )
    )


def register_error_handlers(app: Flask) -> None:
    """Registers error handlers"""
    app.errorhandler(CSRFError)(handle_csrf_error)
    app.errorhandler(ValidationError)(handle_validation_error)
    app.errorhandler(MissingRequiredClaimError)(handle_missing_required_claim_error)
    app.errorhandler(FlaskException)(handle_auth_error)
    app.errorhandler(MetricMappingError)(handle_metric_mapping_error)
    app.errorhandler(MetricNotEnabledError)(handle_metric_not_enabled_error)
