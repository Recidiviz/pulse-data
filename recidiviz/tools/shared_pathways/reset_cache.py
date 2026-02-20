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
"""
Tool for resetting the cache in our Pathways development database.

This script should be run only after `docker-compose up` has been run.

Usage against default development database (docker-compose v1):
docker exec pulse-data_case_triage_backend_1 uv run python -m recidiviz.tools.shared_pathways.reset_cache --schema_type PATHWAYS

Usage against default development database (docker-compose v2):
docker exec pulse-data-public_pathways_backend-1 uv run python -m recidiviz.tools.shared_pathways.reset_cache --schema_type PUBLIC_PATHWAYS
"""
import argparse
import logging

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.calculator.query.state.views.public_pathways.public_pathways_enabled_states import (
    get_public_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.case_triage.pathways.enabled_metrics import (
    ALL_PATHWAYS_METRICS_BY_STATE_CODE,
)
from recidiviz.case_triage.shared_pathways.metric_cache import PathwaysMetricCache
from recidiviz.persistence.database.schema.pathways.schema import (
    MetricMetadata as PathwaysMetricMetadata,
)
from recidiviz.persistence.database.schema.public_pathways.schema import (
    MetricMetadata as PublicPathwaysMetricMetadata,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.public_pathways.enabled_metrics import (
    ALL_PUBLIC_PATHWAYS_METRICS_BY_STATE_CODE,
)
from recidiviz.utils.environment import in_development

if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    parser = argparse.ArgumentParser(description="Reset the Pathways metric cache.")
    parser.add_argument(
        "--schema_type",
        type=SchemaType,
        choices=[SchemaType.PATHWAYS, SchemaType.PUBLIC_PATHWAYS],
        required=True,
        help="The schema type to reset the cache for.",
    )
    args = parser.parse_args()

    schema_type: SchemaType = args.schema_type

    if schema_type == SchemaType.PATHWAYS:
        enabled_metrics_by_state = ALL_PATHWAYS_METRICS_BY_STATE_CODE
        enabled_states = get_pathways_enabled_states_for_cloud_sql()
        metadata_model: type[PathwaysMetricMetadata] | type[
            PublicPathwaysMetricMetadata
        ] = PathwaysMetricMetadata
    elif schema_type == SchemaType.PUBLIC_PATHWAYS:
        enabled_metrics_by_state = ALL_PUBLIC_PATHWAYS_METRICS_BY_STATE_CODE
        enabled_states = get_public_pathways_enabled_states_for_cloud_sql()
        metadata_model = PublicPathwaysMetricMetadata
    else:
        raise ValueError("Schema type must be either PATHWAYS or PUBLIC_PATHWAYS")

    logging.basicConfig(level=logging.INFO)

    for state, metric_list in enabled_metrics_by_state.items():
        if state.value not in enabled_states:
            continue
        metric_cache = PathwaysMetricCache.build(
            state,
            schema_type=schema_type,
            enabled_states=enabled_states,
            metadata_model=metadata_model,
        )
        for metric in metric_list:
            metric_cache.reset_cache(metric)
