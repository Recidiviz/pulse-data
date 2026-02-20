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
"""Utilities for Pathways and Public Pathways routes"""

from typing import List

from recidiviz.case_triage.pathways.metrics.metric_query_builders import (
    ALL_PATHWAYS_METRICS,
)
from recidiviz.case_triage.shared_pathways.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema.public_pathways.schema import (
    PublicPathwaysBase,
)
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS,
)


def get_metrics_for_entity(
    db_entity: PathwaysBase | PublicPathwaysBase,
) -> List[MetricQueryBuilder]:
    metrics = (
        ALL_PATHWAYS_METRICS
        if issubclass(db_entity, PathwaysBase)
        else ALL_PUBLIC_PATHWAYS_METRICS
    )
    return [metric for metric in metrics if metric.model == db_entity]
