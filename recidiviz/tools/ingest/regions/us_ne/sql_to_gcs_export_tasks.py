# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Configuration module for US_NE's SQL Server to GCS export tasks, with a task
representing a single table export/upload."""
import datetime
from collections import defaultdict
from enum import Enum

import attr

from recidiviz.common import attr_validators
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_name,
)

# Most of the file tags are found in the DCS_WEB db
# but the following are only in the DCS_MVS db
DCS_MVS_FILE_TAGS = [
    "E04_LOCT_PRFX",
    "A03_ADMIS_TYPE",
    "CTS_Inmate",
    "A16_INST_RLSE_TYPE",
    "LOCATION_HIST",
    "CUSTODY_HIST",
]

# All file tags correspond to the table name in the database
# except for the following
FILE_TAG_TO_TABLE_NAME = {
    "PIMSSanctions": "view_PIMSSanctions",
    "PIMSSanctionsNoncompliantBehaviors": "view_BMS_tblNoncompliantBehaviors",
    "PIMSSanctionsNoncompliantResponses": "view_BMS_tblNoncompliantResponses",
    "PIMSSanctionsBehaviorEvent": "view_BMS_lnkEventVio",
    "PIMSSanctionsResponseEvent": "view_BMS_lnkEventResponse",
}


class UsNeDatabaseName(Enum):
    """Enum for Nebraska databases."""

    DCS_WEB = "DCS_WEB"
    DCS_MVS = "DCS_MVS"


@attr.define
class UsNeSqltoGCSExportTask:
    """Represents a SQL to GCS export task for a single file tag."""

    file_tag: str = attr.ib(validator=attr_validators.is_str)
    table_name: str = attr.ib(validator=attr_validators.is_str)
    db: UsNeDatabaseName = attr.ib(
        validator=attr.validators.instance_of(UsNeDatabaseName)
    )
    update_datetime: datetime.datetime = attr.ib(validator=attr_validators.is_datetime)

    @classmethod
    def from_file_tag_and_update_dt(
        cls, file_tag: str, update_datetime: datetime.datetime
    ) -> "UsNeSqltoGCSExportTask":
        """Factory to create export tasks for the given file tag and update_datetime.

        We assume that file tags are found in DCS_WEB and have table names
        that match the file tag by default.
        """
        table_name = FILE_TAG_TO_TABLE_NAME.get(file_tag, file_tag)
        db = (
            UsNeDatabaseName.DCS_MVS
            if file_tag in DCS_MVS_FILE_TAGS
            else UsNeDatabaseName.DCS_WEB
        )
        return cls(
            file_tag=file_tag,
            table_name=table_name,
            db=db,
            update_datetime=update_datetime,
        )

    @property
    def file_name(self) -> str:
        return to_normalized_unprocessed_raw_file_name(
            file_name=self.file_tag, dt=self.update_datetime
        )

    def to_str(self) -> str:
        return f"File Tag: {self.file_tag}, Table: {self.db.value}.{self.table_name}"


def sql_to_gcs_export_tasks_by_db(
    file_tags: list[str], update_datetime: datetime.datetime
) -> dict[UsNeDatabaseName, list[UsNeSqltoGCSExportTask]]:
    """Group export tasks by database. Assumes all files have the same update_datetime."""
    export_tasks = [
        UsNeSqltoGCSExportTask.from_file_tag_and_update_dt(file_tag, update_datetime)
        for file_tag in file_tags
    ]
    tasks_by_db: dict[UsNeDatabaseName, list[UsNeSqltoGCSExportTask]] = defaultdict(
        list
    )
    for task in export_tasks:
        tasks_by_db[task.db].append(task)

    return tasks_by_db
