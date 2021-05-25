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
"""Defines admin panel routes for ingest operations."""
from http import HTTPStatus
from typing import Tuple

from flask import Blueprint, jsonify, request

from recidiviz.admin_panel.admin_stores import AdminStores
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.auth.gae import requires_gae_auth


def _get_state_code_from_str(state_code_str: str) -> StateCode:
    if not StateCode.is_state_code(state_code_str):
        raise ValueError(
            f"Unknown region_code [{state_code_str}] received, must be a valid state code."
        )

    return StateCode[state_code_str.upper()]


def add_ingest_ops_routes(bp: Blueprint, admin_stores: AdminStores) -> None:
    """Adds routes for ingest operations."""

    @bp.route("/api/ingest_operations/fetch_ingest_state_codes", methods=["POST"])
    @requires_gae_auth
    def _fetch_ingest_state_codes() -> Tuple[str, HTTPStatus]:
        all_state_codes = (
            admin_stores.ingest_operations_store.state_codes_launched_in_env
        )
        state_code_info = []
        for state_code in all_state_codes:
            code_to_name = {
                "code": state_code.value,
                "name": state_code.get_state().name,
            }
            state_code_info.append(code_to_name)
        return jsonify(state_code_info), HTTPStatus.OK

    # Start an ingest run for a specific instance
    @bp.route(
        "/api/ingest_operations/<state_code_str>/start_ingest_run", methods=["POST"]
    )
    @requires_gae_auth
    def _start_ingest_run(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        instance = request.json["instance"]
        admin_stores.ingest_operations_store.start_ingest_run(state_code, instance)
        return "", HTTPStatus.OK

    # Update ingest queues
    @bp.route(
        "/api/ingest_operations/<state_code_str>/update_ingest_queues_state",
        methods=["POST"],
    )
    @requires_gae_auth
    def _update_ingest_queues_state(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        new_queue_state = request.json["new_queue_state"]
        admin_stores.ingest_operations_store.update_ingest_queues_state(
            state_code, new_queue_state
        )
        return "", HTTPStatus.OK

    # Get all ingest queues and their state for given state code
    @bp.route("/api/ingest_operations/<state_code_str>/get_ingest_queue_states")
    @requires_gae_auth
    def _get_ingest_queue_states(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        ingest_queue_states = (
            admin_stores.ingest_operations_store.get_ingest_queue_states(state_code)
        )
        return jsonify(ingest_queue_states), HTTPStatus.OK

    # Get summaries of all ingest instances for state
    @bp.route("/api/ingest_operations/<state_code_str>/get_ingest_instance_summaries")
    @requires_gae_auth
    def _get_ingest_instance_summaries(state_code_str: str) -> Tuple[str, HTTPStatus]:
        state_code = _get_state_code_from_str(state_code_str)
        ingest_instance_summaries = (
            admin_stores.ingest_operations_store.get_ingest_instance_summaries(
                state_code
            )
        )
        return jsonify(ingest_instance_summaries), HTTPStatus.OK
