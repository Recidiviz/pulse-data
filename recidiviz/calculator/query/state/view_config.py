# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Dashboard view configuration."""

# Where the dashboard views and materialized tables live
from typing import Dict, List

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state.dataset_config import REFERENCE_TABLES_DATASET, DASHBOARD_VIEWS_DATASET
from recidiviz.calculator.query.state.views.admissions import admissions_views
from recidiviz.calculator.query.state.views.program_evaluation import program_evaluation_views
from recidiviz.calculator.query.state.views.reference import reference_views
from recidiviz.calculator.query.state.views.reincarcerations import reincarcerations_views
from recidiviz.calculator.query.state.views.revocation_analysis import revocation_analysis_views
from recidiviz.calculator.query.state.views.revocations import revocations_views
from recidiviz.calculator.query.state.views.supervision import supervision_views


VIEWS_TO_UPDATE: Dict[str, List[BigQueryView]] = {
    REFERENCE_TABLES_DATASET: reference_views.REF_VIEWS,
    DASHBOARD_VIEWS_DATASET: (
        admissions_views.ADMISSIONS_VIEWS +
        reincarcerations_views.REINCARCERATIONS_VIEWS +
        revocations_views.REVOCATIONS_VIEWS +
        supervision_views.SUPERVISION_VIEWS +
        program_evaluation_views.PROGRAM_EVALUATION_VIEWS +
        revocation_analysis_views.REVOCATION_ANALYSIS_VIEWS
    )
}
