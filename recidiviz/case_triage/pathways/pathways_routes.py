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
"""Implements routes for the Pathways Flask blueprint. """
from flask import Blueprint

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_PATHWAYS_METRICS,
)
from recidiviz.case_triage.pathways.pathways_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.shared_pathways.shared_pathways_blueprint import (
    SharedPathwaysBlueprint,
)
from recidiviz.persistence.database.schema.pathways.schema import MetricMetadata
from recidiviz.persistence.database.schema_type import SchemaType

PATHWAYS_ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
]


def create_pathways_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Pathways"""
    return SharedPathwaysBlueprint(
        blueprint_name="pathways",
        auth_handler_name="dashboard_auth0",
        on_successful_authorization=on_successful_authorization,
        allowed_origins=PATHWAYS_ALLOWED_ORIGINS,
        schema_type=SchemaType.PATHWAYS,
        enabled_states=get_pathways_enabled_states_for_cloud_sql(),
        enabled_metrics=ALL_PATHWAYS_METRICS,
        metric_metadata=MetricMetadata,
    ).api
