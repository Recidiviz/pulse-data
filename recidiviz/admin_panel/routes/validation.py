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
"""Defines routes for the Validation API the admin panel."""
from http import HTTPStatus
from typing import Tuple

from flask import Blueprint, Response, jsonify, request

from recidiviz.admin_panel.admin_stores import (
    fetch_state_codes,
    get_validation_status_store,
)
from recidiviz.utils.types import assert_type
from recidiviz.validation.configured_validations import get_all_validations_by_name


def add_validation_routes(admin_panel: Blueprint) -> None:
    """Adds the relevant Validation API routes to an input Blueprint."""

    # Validation status
    @admin_panel.route("/api/validation_metadata/state_codes", methods=["POST"])
    def fetch_validation_state_codes() -> Tuple[Response, HTTPStatus]:
        all_state_codes = get_validation_status_store().state_codes
        state_code_info = fetch_state_codes(all_state_codes)
        return jsonify(state_code_info), HTTPStatus.OK

    @admin_panel.route("/api/validation_metadata/status", methods=["POST"])
    def fetch_validation_metadata_status() -> Tuple[bytes, HTTPStatus]:
        records = get_validation_status_store().get_most_recent_validation_results()
        return (
            records.SerializeToString(),
            HTTPStatus.OK,
        )

    @admin_panel.route(
        "/api/validation_metadata/status/<validation_name>/<state_code>",
        methods=["POST"],
    )
    def fetch_validation_metadata_status_for_validation(
        validation_name: str, state_code: str
    ) -> Tuple[bytes, HTTPStatus]:
        request_json = assert_type(request.json, dict)
        lookbackDays = request_json["lookbackDays"]
        records = get_validation_status_store().get_results_for_validation(
            validation_name, state_code, lookbackDays
        )
        return (
            records.SerializeToString(),
            HTTPStatus.OK,
        )

    @admin_panel.route(
        "/api/validation_metadata/error_table/<validation_name>/<state_code>",
        methods=["POST"],
    )
    def fetch_validation_metadata_error_table_for_validation(
        validation_name: str, state_code: str
    ) -> Tuple[str, HTTPStatus]:
        records = get_validation_status_store().get_error_table_for_validation(
            validation_name, state_code
        )

        return records, HTTPStatus.OK

    @admin_panel.route(
        "/api/validation_metadata/description/<validation_name>", methods=["POST"]
    )
    def fetch_validation_description(validation_name: str) -> Tuple[str, HTTPStatus]:
        validations = get_all_validations_by_name()
        if validation_name not in validations:
            return (
                f"No validation name matches the name {validation_name}",
                HTTPStatus.BAD_REQUEST,
            )

        return validations[validation_name].view_builder.description, HTTPStatus.OK
