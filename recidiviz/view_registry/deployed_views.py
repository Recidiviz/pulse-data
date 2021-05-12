# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Views that are regularly updated with the deploy and rematerialized with metric exports.."""
from typing import Dict, Sequence, List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.county.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as COUNTY_VIEW_BUILDERS,
)
from recidiviz.calculator.query.experiments.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as EXPERIMENTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.justice_counts.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as JUSTICE_COUNTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as STATE_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_daily_job_id_by_metric_and_state_code import (
    MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_job_id_by_metric_and_state_code import (
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER,
)
from recidiviz.case_triage.views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as CASE_TRIAGE_VIEW_BUILDERS,
)
from recidiviz.ingest.direct.views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as DIRECT_INGEST_VIEW_BUILDERS,
)
from recidiviz.ingest.views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as INGEST_METADATA_VIEW_BUILDERS,
)
from recidiviz.validation.views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as VALIDATION_VIEW_BUILDERS,
    METADATA_VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as VALIDATION_METADATA_VIEW_BUILDERS,
)
from recidiviz.view_registry.namespaces import BigQueryViewNamespace

DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE: Dict[
    BigQueryViewNamespace, Sequence[BigQueryViewBuilder]
] = {
    BigQueryViewNamespace.COUNTY: COUNTY_VIEW_BUILDERS,
    BigQueryViewNamespace.EXPERIMENTS: EXPERIMENTS_VIEW_BUILDERS,
    BigQueryViewNamespace.JUSTICE_COUNTS: JUSTICE_COUNTS_VIEW_BUILDERS,
    BigQueryViewNamespace.DIRECT_INGEST: DIRECT_INGEST_VIEW_BUILDERS,
    BigQueryViewNamespace.STATE: STATE_VIEW_BUILDERS,
    BigQueryViewNamespace.VALIDATION: VALIDATION_VIEW_BUILDERS,
    BigQueryViewNamespace.CASE_TRIAGE: CASE_TRIAGE_VIEW_BUILDERS,
    BigQueryViewNamespace.INGEST_METADATA: INGEST_METADATA_VIEW_BUILDERS,
    BigQueryViewNamespace.VALIDATION_METADATA: VALIDATION_METADATA_VIEW_BUILDERS,
}

# Full list of all deployed view builders
DEPLOYED_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    builder
    for builder_list in DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE.values()
    for builder in builder_list
]

# TODO(#7049): refactor most_recent_job_id_by_metric_and_state_code dependencies
NOISY_DEPENDENCY_VIEW_BUILDERS = {
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER,
    MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER,
}
