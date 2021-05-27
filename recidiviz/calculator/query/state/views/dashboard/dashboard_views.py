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
"""All views that populate the data in the dashboards."""
import itertools
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.admissions import admissions_views
from recidiviz.calculator.query.state.views.dashboard.program_evaluation import (
    program_evaluation_views,
)
from recidiviz.calculator.query.state.views.dashboard.reincarcerations import (
    reincarcerations_views,
)
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis import (
    revocation_analysis_views,
)
from recidiviz.calculator.query.state.views.dashboard.revocations import (
    revocations_views,
)
from recidiviz.calculator.query.state.views.dashboard.supervision import (
    supervision_views,
)
from recidiviz.calculator.query.state.views.dashboard.population_projections import (
    population_projections_views,
)
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries import (
    vitals_views,
)

from recidiviz.calculator.query.state.views.reference.supervision_location_restricted_access_emails import (
    SUPERVISION_LOCATION_RESTRICTED_ACCESS_EMAILS_VIEW_BUILDER,
)

CORE_DASHBOARD_VIEW_BUILDERS: Sequence[BigQueryViewBuilder] = (
    admissions_views.ADMISSIONS_VIEW_BUILDERS
    + reincarcerations_views.REINCARCERATIONS_VIEW_BUILDERS
    + revocations_views.REVOCATIONS_VIEW_BUILDERS
    + supervision_views.SUPERVISION_VIEW_BUILDERS
    + program_evaluation_views.PROGRAM_EVALUATION_VIEW_BUILDERS
)


LANTERN_DASHBOARD_VIEW_BUILDERS: Sequence[
    BigQueryViewBuilder
] = revocation_analysis_views.REVOCATION_ANALYSIS_VIEW_BUILDERS


UP_DASHBOARD_VIEW_BUILDERS: Sequence[BigQueryViewBuilder] = (
    population_projections_views.POPULATION_PROJECTION_VIEW_BUILDERS
    + vitals_views.VITALS_VIEW_BUILDERS
)

DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDERS: Sequence[
    SelectedColumnsBigQueryViewBuilder
] = [SUPERVISION_LOCATION_RESTRICTED_ACCESS_EMAILS_VIEW_BUILDER]


DASHBOARD_VIEW_BUILDERS: Sequence[BigQueryViewBuilder] = list(
    itertools.chain.from_iterable(
        (
            CORE_DASHBOARD_VIEW_BUILDERS,
            LANTERN_DASHBOARD_VIEW_BUILDERS,
            UP_DASHBOARD_VIEW_BUILDERS,
        )
    )
)
