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
import csv
import datetime
from enum import Enum

import attr

from recidiviz.common import attr_validators
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_name,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
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
    columns: list[str] = attr.ib(validator=attr_validators.is_list)

    encoding: str = attr.ib(validator=attr_validators.is_str)
    line_terminator: str = attr.ib(validator=attr_validators.is_str)
    delimiter: str = attr.ib(validator=attr_validators.is_str)
    quoting_mode: int = attr.ib(validator=attr_validators.is_int)

    db: UsNeDatabaseName = attr.ib(
        validator=attr.validators.instance_of(UsNeDatabaseName)
    )
    update_datetime: datetime.datetime = attr.ib(validator=attr_validators.is_datetime)

    @classmethod
    def for_file_tag(
        cls,
        file_tag: str,
        update_datetime: datetime.datetime,
        raw_file_config: DirectIngestRawFileConfig,
    ) -> "UsNeSqltoGCSExportTask":
        """Factory to create export tasks for the given file tag, update_datetime, and raw_file_config.

        We assume that file tags are found in DCS_WEB and have table names
        that match the file tag by default.

        If we do not have a raw file config for the file tag, we use the default
        config for the region.
        """
        if raw_file_config.infer_columns_from_config:
            raise ValueError(
                f"File tag [{raw_file_config.file_tag}]: did not expect any files with infer_columns_from_config=True in NE"
            )
        if not raw_file_config.ignore_quotes:
            raise ValueError(
                f"File tag [{raw_file_config.file_tag}]: did not expect any files with ignore_quotes=False in NE"
            )

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
            encoding=raw_file_config.encoding,
            line_terminator=raw_file_config.line_terminator,
            delimiter=raw_file_config.separator,
            quoting_mode=raw_file_config.quoting_mode,
            columns=[col.name for col in raw_file_config.current_columns],
        )

    @classmethod
    def for_qualified_table_name(
        cls,
        qualified_table_name: str,
        update_datetime: datetime.datetime,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
    ) -> "UsNeSqltoGCSExportTask":
        """Factory to create export tasks for the given qualified table name and update_datetime.

        The qualified table name is expected to be in the format 'db_name.table_name'.

        If the table name is not found in the raw file tags, we select all columns by default and use
        the region's default config for the encoding, line terminator, and delimiter. If there is no
        default line terminator, we use '\n' as the default.
        """
        parts = qualified_table_name.split(".")
        if len(parts) != 2:
            raise ValueError(
                f"Qualified table name must be in the format 'db_name.table_name', got {qualified_table_name}"
            )

        db_name = parts[0]
        table_name = parts[1]
        if table_name in region_raw_file_config.raw_file_tags:
            return cls.for_file_tag(
                file_tag=table_name,
                update_datetime=update_datetime,
                raw_file_config=region_raw_file_config.raw_file_configs[table_name],
            )

        default_region_config = region_raw_file_config.default_config()
        return cls(
            file_tag=table_name,
            table_name=table_name,
            db=UsNeDatabaseName(db_name),
            update_datetime=update_datetime,
            encoding=default_region_config.default_encoding,
            line_terminator=default_region_config.default_line_terminator or "\n",
            delimiter=default_region_config.default_separator,
            quoting_mode=(
                csv.QUOTE_NONE
                if default_region_config.default_ignore_quotes
                else csv.QUOTE_MINIMAL
            ),
            columns=["*"],
        )

    @property
    def file_name(self) -> str:
        return to_normalized_unprocessed_raw_file_name(
            file_name=self.file_tag, dt=self.update_datetime
        )

    def to_str(self) -> str:
        return f"File Tag: {self.file_tag}, Table: {self.db.value}.{self.table_name}"
