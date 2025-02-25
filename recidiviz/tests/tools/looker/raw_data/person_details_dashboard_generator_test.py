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
"""Unit tests for person details LookML Dashboard generation"""

from types import ModuleType
from typing import List

from freezegun import freeze_time

from recidiviz.tests.tools.looker.raw_data.person_details_generator_test_utils import (
    PersonDetailsLookMLGeneratorTest,
)
from recidiviz.tools.looker.raw_data import (
    person_details_dashboard_generator,
    person_details_explore_generator,
    person_details_view_generator,
)
from recidiviz.tools.looker.raw_data.person_details_dashboard_generator import (
    generate_lookml_dashboards,
)
from recidiviz.tools.looker.raw_data.person_details_explore_generator import (
    _generate_all_state_explores,
)
from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    _generate_state_raw_data_views,
)


class LookMLDashboardTest(PersonDetailsLookMLGeneratorTest):
    """Tests LookML dashboard generation functions"""

    @classmethod
    def generator_modules(cls) -> List[ModuleType]:
        return [
            person_details_view_generator,
            person_details_explore_generator,
            person_details_dashboard_generator,
        ]

    @freeze_time("2000-06-30")
    def test_generate_lookml_dashboards(self) -> None:
        all_views = _generate_state_raw_data_views()
        all_explores = _generate_all_state_explores(all_views)
        self.generate_files(
            function_to_test=lambda s: generate_lookml_dashboards(
                s, all_views, all_explores
            ),
            filename_filter=".dashboard.lookml",
        )
