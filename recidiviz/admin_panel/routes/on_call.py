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
"""Defines routes for the on-call section of the admin panel."""

from flask import Blueprint, Response, jsonify, request

from recidiviz.admin_panel.on_call.on_call_logs_search import LogsView, OnCallLogsSearch


def add_on_call_routes(blueprint: Blueprint) -> None:
    @blueprint.route("/on_call/logs", methods=["POST"])
    def logs() -> Response:
        """
        Request Body:
            view: (string) LogsView enum value controls which logs are returned
        Returns:
            JSON representation of the logs
        """
        view = LogsView.DIRECT_INGEST
        if not request.json:
            raise ValueError("Must pass request JSON!")

        if request.json and "view" in request.json:
            try:
                view = LogsView(request.json["view"])
            except ValueError:
                pass

        return jsonify(
            OnCallLogsSearch().query(
                view,
                ignored_statuses=request.json["ignored_statuses"],
                cloud_run_services=request.json.get("cloud_run_services", ""),
                show_resolved=request.json.get("show_resolved"),
            )
        )
