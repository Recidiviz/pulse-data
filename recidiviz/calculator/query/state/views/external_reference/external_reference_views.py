# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Views on top of the external reference data"""


from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.external_reference.state_info import (
    get_state_info_view_builder,
)
from recidiviz.calculator.query.state.views.external_reference.state_resident_population import (
    STATE_RESIDENT_POPULATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.external_reference.state_resident_population_combined_race_ethnicity import (
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.external_reference.state_resident_population_combined_race_ethnicity_priority import (
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_PRIORITY_VIEW_BUILDER,
)

EXTERNAL_REFERENCE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    get_state_info_view_builder(),
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_VIEW_BUILDER,
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_PRIORITY_VIEW_BUILDER,
    STATE_RESIDENT_POPULATION_VIEW_BUILDER,
]
