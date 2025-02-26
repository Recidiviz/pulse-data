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
"""Unit tests for person details LookML View generation"""
from types import ModuleType

from freezegun import freeze_time

from recidiviz.tests.tools.looker.raw_data.person_details_generator_test_utils import (
    PersonDetailsLookMLGeneratorTest,
)
from recidiviz.tools.looker.raw_data import person_details_view_generator
from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    generate_lookml_views,
)


class LookMLViewTest(PersonDetailsLookMLGeneratorTest):
    """Tests LookML view generation functions"""

    @classmethod
    def generator_module(cls) -> ModuleType:
        return person_details_view_generator

    @freeze_time("2000-06-30")
    def test_generate_lookml_views(self) -> None:
        self.generate_files(
            function_to_test=generate_lookml_views,
            filename_filter=".view.lkml",
        )
