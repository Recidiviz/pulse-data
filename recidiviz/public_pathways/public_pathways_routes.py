# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Implements routes for the Public Pathways Flask blueprint. """
from flask import Blueprint, Response, request
from flask_limiter import Limiter
from werkzeug.exceptions import BadRequest
from werkzeug.http import parse_set_header

from recidiviz.calculator.query.state.views.public_pathways.public_pathways_enabled_states import (
    get_public_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.case_triage.shared_pathways.shared_pathways_blueprint import (
    SharedPathwaysBlueprint,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.public_pathways.schema import MetricMetadata
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.public_pathways.individual_level_bulk_export import (
    build_bulk_individual_level_export_response,
)
from recidiviz.public_pathways.individual_level_export import (
    build_individual_level_export_response,
)
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS,
)
from recidiviz.public_pathways.public_pathways_authorization import (
    INDIVIDUAL_LEVEL_EXPORT_VIEW_ARG,
    on_successful_authorization,
)


def _parse_optional_year_month() -> tuple[int, int] | None:
    """Returns the (year, month) pair from the request's ?year=&month= query
    params, or None if neither is present. Raises BadRequest if only one is
    present, or if either fails to parse as an int.
    """
    year_str = request.args.get("year")
    month_str = request.args.get("month")
    if year_str is None and month_str is None:
        return None
    if year_str is None or month_str is None:
        raise BadRequest("year and month must both be provided, or neither")

    try:
        return int(year_str), int(month_str)
    except ValueError as e:
        raise BadRequest(f"Invalid year/month: [{year_str}]/[{month_str}]") from e


PUBLIC_PATHWAYS_ALLOWED_ORIGINS = [
    r"http\://localhost:3050",
    r"https\://pathways-staging\.recidiviz\.org$",
    r"https\://pathways\.recidiviz\.org$",
    r"https\://dashboards\.doccs\.ny\.gov$",
    r"https\://public-pathways-staging--[a-zA-Z0-9-]+-[a-z0-9]{8}\.web\.app$",
]


def create_public_pathways_api_blueprint(limiter: Limiter) -> Blueprint:
    """Creates the API blueprint for Public Pathways"""
    enabled_states = get_public_pathways_enabled_states_for_cloud_sql()
    blueprint = SharedPathwaysBlueprint(
        blueprint_name="public_pathways",
        auth_handler_name="public_pathways_auth0",
        on_successful_authorization=on_successful_authorization,
        allowed_origins=PUBLIC_PATHWAYS_ALLOWED_ORIGINS,
        schema_type=SchemaType.PUBLIC_PATHWAYS,
        enabled_states=enabled_states,
        enabled_metrics=ALL_PUBLIC_PATHWAYS_METRICS,
        metric_metadata=MetricMetadata,
        skip_authentication_in_production=True,
    ).api

    @blueprint.after_request
    def expose_file_download_headers(response: Response) -> Response:
        # Lets the frontend read the filename off of file download responses,
        # e.g. the individual-level data export below. Scoped to this
        # blueprint only, since no other Pathways product returns files.
        response.access_control_expose_headers = parse_set_header("Content-Disposition")
        return response

    def _validated_state_code(state: str) -> StateCode:
        try:
            state_code = StateCode(state)
        except ValueError as e:
            raise BadRequest(f"Invalid state code: [{state}]") from e

        if state_code.value not in enabled_states:
            raise BadRequest(
                f"Public Pathways is not enabled for state: [{state_code.value}]"
            )
        return state_code

    @blueprint.get(
        "/<state>/PrisonPopulationIndividualLevel",
        defaults={INDIVIDUAL_LEVEL_EXPORT_VIEW_ARG: True},
    )
    @limiter.limit("10 per minute")  # type: ignore[attr-defined]
    def individual_level_export(
        state: str,
        # Name must match INDIVIDUAL_LEVEL_EXPORT_VIEW_ARG: Flask calls this
        # function with **view_args, so the defaults key above is passed as a
        # keyword argument matched by name.
        individual_level_export: bool,  # pylint: disable=unused-argument
    ) -> Response:
        state_code = _validated_state_code(state)
        year_month = _parse_optional_year_month()

        return build_individual_level_export_response(
            state_code=state_code,
            enabled_states=enabled_states,
            year_month=year_month,
        )

    @blueprint.get(
        "/<state>/PrisonPopulationIndividualLevelBulk",
        defaults={INDIVIDUAL_LEVEL_EXPORT_VIEW_ARG: True},
    )
    @limiter.limit("10 per minute")  # type: ignore[attr-defined]
    def individual_level_export_bulk(
        state: str,
        # Name must match INDIVIDUAL_LEVEL_EXPORT_VIEW_ARG: Flask calls this
        # function with **view_args, so the defaults key above is passed as a
        # keyword argument matched by name.
        individual_level_export: bool,  # pylint: disable=unused-argument
    ) -> Response:
        state_code = _validated_state_code(state)
        return build_bulk_individual_level_export_response(state_code=state_code)

    return blueprint
