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
from flask import Blueprint

from recidiviz.calculator.query.state.views.public_pathways.public_pathways_enabled_states import (
    get_public_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.case_triage.shared_pathways.shared_pathways_blueprint import (
    SharedPathwaysBlueprint,
)
from recidiviz.persistence.database.schema.public_pathways.schema import MetricMetadata
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS,
)
from recidiviz.public_pathways.public_pathways_authorization import (
    on_successful_authorization,
)

PUBLIC_PATHWAYS_ALLOWED_ORIGINS = [
    r"http\://localhost:3050",
    r"https\://pathways-staging\.recidiviz\.org$",
    r"https\://pathways\.recidiviz\.org$",
]


def create_public_pathways_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Public Pathways"""
    return SharedPathwaysBlueprint(
        blueprint_name="public_pathways",
        auth_handler_name="public_pathways_auth0",
        on_successful_authorization=on_successful_authorization,
        allowed_origins=PUBLIC_PATHWAYS_ALLOWED_ORIGINS,
        schema_type=SchemaType.PUBLIC_PATHWAYS,
        enabled_states=get_public_pathways_enabled_states_for_cloud_sql(),
        enabled_metrics=ALL_PUBLIC_PATHWAYS_METRICS,
        metric_metadata=MetricMetadata,
    ).api
