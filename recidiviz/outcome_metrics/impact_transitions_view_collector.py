# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
Defines a class that can be used to collect view builders of type
ImpactTransitionsBigQueryViewBuilder.
"""

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.outcome_metrics.impact_transitions_view_builder import (
    ImpactTransitionsBigQueryViewBuilder,
)
from recidiviz.outcome_metrics.views import (
    transitions_view_builders as impact_transitions,
)


class ImpactTransitionsBigQueryViewCollector(
    BigQueryViewCollector[ImpactTransitionsBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of type
    ImpactTransitionsBigQueryViewBuilder.
    """

    def collect_view_builders(
        self,
    ) -> list[ImpactTransitionsBigQueryViewBuilder]:
        builders = self.collect_view_builders_in_module(
            builder_type=ImpactTransitionsBigQueryViewBuilder,
            view_dir_module=impact_transitions,
        )

        return builders
