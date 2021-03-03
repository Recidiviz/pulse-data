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
"""A test view builder file for big_query_view_collector_test.py"""

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.tests.big_query.fake_big_query_view_builder import (
    FakeBigQueryViewBuilder,
)
from recidiviz.utils.metadata import local_project_id_override

with local_project_id_override("my-project-id"):
    GOOD_VIEW_1 = BigQueryView(
        dataset_id="my_dataset",
        view_id="early_discharge_incarceration_sentence",
        view_query_template="SELECT * FROM table1",
    )

VIEW_BUILDER = FakeBigQueryViewBuilder(GOOD_VIEW_1)
