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
"""Base class for generating a query to compare raw data between tables."""
import abc
from typing import Optional

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


@attr.define
class RawTableDiffQueryGenerator:
    """Base class for generating a query to compare raw data between tables."""

    region_code: str
    src_project_id: str
    src_ingest_instance: DirectIngestInstance
    cmp_project_id: str
    cmp_ingest_instance: DirectIngestInstance
    truncate_update_datetime_part: Optional[str] = None

    src_dataset_id: str = attr.ib(init=False)
    cmp_dataset_id: str = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.src_dataset_id = self._get_dataset_id_for_ingest_instance(
            self.src_ingest_instance
        )
        self.cmp_dataset_id = self._get_dataset_id_for_ingest_instance(
            self.cmp_ingest_instance
        )

    def _get_dataset_id_for_ingest_instance(
        self, ingest_instance: DirectIngestInstance
    ) -> str:
        state_code = StateCode(self.region_code.upper())

        return raw_tables_dataset_for_region(
            state_code=state_code,
            instance=ingest_instance,
            sandbox_dataset_prefix=None,
        )

    def _get_truncated_datetime_column_name(self) -> str:
        return (
            f"DATETIME_TRUNC({UPDATE_DATETIME_COL_NAME}, {self.truncate_update_datetime_part}) as {UPDATE_DATETIME_COL_NAME}"
            if self.truncate_update_datetime_part
            else UPDATE_DATETIME_COL_NAME
        )

    @abc.abstractmethod
    def generate_query(self, file_tag: str) -> str:
        """Generate the query to compare the tables."""
