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
"""The engineering metrics views that the view update deploys."""
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.monitoring.eng_metrics.pr_first_feedback import (
    PR_FIRST_FEEDBACK_VIEW_BUILDER,
)


def get_eng_metrics_views_to_update() -> list[BigQueryViewBuilder]:
    """Returns every engineering metrics view builder, in dependency order."""
    return [PR_FIRST_FEEDBACK_VIEW_BUILDER]
