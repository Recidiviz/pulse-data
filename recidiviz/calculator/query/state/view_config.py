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
from recidiviz.calculator.query.state.views.covid_dashboard.covid_dashboard_views import COVID_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.dataflow_metrics_materialized_views import \
    DATAFLOW_METRICS_MATERIALIZED_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.covid_report.covid_report_views import COVID_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.po_report.po_report_views import PO_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import \
    PUBLIC_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.reference.reference_views import REFERENCE_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.analyst_data.analyst_data_views import ANALYST_DATA_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.population_projection.population_projection_views import \
    POPULATION_PROJECTION_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.vitals_report.vitals_report_views import VITALS_REPORT_VIEW_BUILDERS


VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = list(itertools.chain.from_iterable((
    DATAFLOW_METRICS_MATERIALIZED_VIEW_BUILDERS,
    REFERENCE_VIEW_BUILDERS,
    COVID_DASHBOARD_VIEW_BUILDERS,
    COVID_REPORT_VIEW_BUILDERS,
    DASHBOARD_VIEW_BUILDERS,
    PO_REPORT_VIEW_BUILDERS,
    PUBLIC_DASHBOARD_VIEW_BUILDERS,
    ANALYST_DATA_VIEW_BUILDERS,
    POPULATION_PROJECTION_VIEW_BUILDERS,
    VITALS_REPORT_VIEW_BUILDERS,
)))
