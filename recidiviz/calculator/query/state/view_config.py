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
"""List of all state view builders to be regularly updated."""
import itertools
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.analyst_data_views import (
    ANALYST_DATA_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import (
    DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_HELPER_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.dataflow_metrics_materialized_views import (
    DATAFLOW_METRICS_MATERIALIZED_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.jii_texting.jii_texting_views import (
    JII_TEXTING_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    OUTLIERS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.population_projection.population_projection_views import (
    POPULATION_PROJECTION_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.case_note_search_views import (
    CASE_NOTE_SEARCH_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import (
    PUBLIC_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.reentry.reentry_views import (
    REENTRY_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.reference.reference_views import (
    REFERENCE_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_sessions_views import (
    SENTENCE_SESSIONS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.sentence_sessions_v2_all.sentence_sessions_v2_all_views import (
    SENTENCE_SESSIONS_V2_ALL_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.sentencing.sentencing_views import (
    CASE_INSIGHTS_VIEW_BUILDERS,
    SENTENCING_HISTORICAL_VIEW_BUILDERS,
    SENTENCING_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.sessions.sessions_views import (
    SESSIONS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.sessions_validation.sessions_validation_views import (
    SESSIONS_VALIDATION_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.shared_metric.shared_metric_views import (
    SHARED_METRIC_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.user_metrics.user_metrics_views import (
    USAGE_REPORTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.vitals_report.vitals_report_views import (
    VITALS_REPORT_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.workflows.workflows_views import (
    WORKFLOWS_VIEW_BUILDERS,
)

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = list(
    itertools.chain.from_iterable(
        (
            ANALYST_DATA_VIEW_BUILDERS,
            DASHBOARD_VIEW_BUILDERS,
            DATAFLOW_METRICS_MATERIALIZED_VIEW_BUILDERS,
            PATHWAYS_HELPER_VIEW_BUILDERS,
            POPULATION_PROJECTION_VIEW_BUILDERS,
            WORKFLOWS_VIEW_BUILDERS,
            PUBLIC_DASHBOARD_VIEW_BUILDERS,
            REFERENCE_VIEW_BUILDERS,
            SESSIONS_VIEW_BUILDERS,
            SESSIONS_VALIDATION_VIEW_BUILDERS,
            SHARED_METRIC_VIEW_BUILDERS,
            VITALS_REPORT_VIEW_BUILDERS,
            OUTLIERS_VIEW_BUILDERS,
            CASE_INSIGHTS_VIEW_BUILDERS,
            SENTENCING_HISTORICAL_VIEW_BUILDERS,
            SENTENCING_VIEW_BUILDERS,
            CASE_NOTE_SEARCH_VIEW_BUILDERS,
            SENTENCE_SESSIONS_VIEW_BUILDERS,
            SENTENCE_SESSIONS_V2_ALL_VIEW_BUILDERS,
            JII_TEXTING_VIEW_BUILDERS,
            USAGE_REPORTS_VIEW_BUILDERS,
            REENTRY_VIEW_BUILDERS,
        )
    )
)
