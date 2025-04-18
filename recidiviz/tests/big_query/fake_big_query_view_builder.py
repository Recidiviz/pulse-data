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
"""A test-only implementation of the BigQueryViewBuilder that just accepts an already built view as a constructor arg.
"""

from recidiviz.big_query.big_query_view import BigQueryViewBuilder, BigQueryViewType
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)


class FakeBigQueryViewBuilder(BigQueryViewBuilder[BigQueryViewType]):
    def __init__(self, view: BigQueryViewType):
        self.view = view

        self.dataset_id = view.address.dataset_id
        self.view_id = view.address.table_id

    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> BigQueryViewType:
        return self.view

    def build_and_print(self) -> None:
        print(self.view.view_query)
