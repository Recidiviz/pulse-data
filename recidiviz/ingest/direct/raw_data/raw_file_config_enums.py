#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Contains enums related to raw file configs."""
from enum import Enum
from typing import Set


class RawDataClassification(Enum):
    """Defines whether this is source or validation data.

    Used to keep the two sets of data separate. This prevents validation data from being
    ingested, or source data from being used to validate our metrics.
    """

    # Data to be ingested and used as the basis of our entities and calcs.
    SOURCE = "source"

    # Used to validate our entities and calcs.
    VALIDATION = "validation"


# TODO(#40717) Add BIRTHDATE
class RawTableColumnFieldType(Enum):
    """Field type for a single raw data column"""

    # Contains string values (this is the default)
    STRING = "string"

    # Contains values representing dates or datetimes
    DATETIME = "datetime"

    # Contains values representing integers
    INTEGER = "integer"

    # Contains external ids representing a justice-impacted individual
    # i.e. values that might hydrate state_person_external_id
    PERSON_EXTERNAL_ID = "person_external_id"

    # Contains external ids representing an agent / staff member
    # i.e. values that might hydrate state_staff_external_id
    STAFF_EXTERNAL_ID = "staff_external_id"

    @classmethod
    def external_id_types(cls) -> Set["RawTableColumnFieldType"]:
        return {cls.PERSON_EXTERNAL_ID, cls.STAFF_EXTERNAL_ID}


class RawDataFileUpdateCadence(Enum):
    """Defines an expected update cadence for a raw data file (i.e. how often we
    expect a state to transfer a raw data file to our ingest infrastructure). We do not
    necessarily expect for new data to flow through our system at this cadence (i.e.
    some states might send files that only contain header rows if there are sending
    incremental updates).
    """

    # There is no defined update cadence or the update cadence is expected to be irregular
    IRREGULAR = "IRREGULAR"

    # The file is expected to be updated once per month
    MONTHLY = "MONTHLY"

    # The file is expected to be updated once per week
    WEEKLY = "WEEKLY"

    # The file is expected to be updated once per day
    DAILY = "DAILY"

    @staticmethod
    def interval_from_cadence(cadence: "RawDataFileUpdateCadence") -> int:
        match cadence:
            case RawDataFileUpdateCadence.DAILY:
                return 1
            case RawDataFileUpdateCadence.WEEKLY:
                return 7
            case RawDataFileUpdateCadence.MONTHLY:
                return 31  # ¯\_(ツ)_/¯
            case _:
                raise ValueError(
                    f"Don't know how to get max days allowed stale for {cadence}"
                )


class RawDataExportLookbackWindow(Enum):
    """Defines the lookback window for each raw data file, or how much historical data we
    expect to be in a typical raw data file a state sends to us. Generally, we prefer
    states to send us full historical files. If a raw file tag is an incremental file,
    that likely means that either the file or underlying data table itself has an audit
    column that reliably allows the states to generate date-bounded diffs.
    """

    # We expect each raw data file to include a full table export, and will never change
    # to incremental exports.
    FULL_HISTORICAL_LOOKBACK = "FULL_HISTORICAL_LOOKBACK"

    # We expect each raw data file to ONLY include the last two months worth of data for this
    # file tag. This means that this file tag or table itself likely has some sort of
    # audit column that allows the state to generate date-bounded diffs.
    TWO_MONTH_INCREMENTAL_LOOKBACK = "TWO_MONTH_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to ONLY include data for the last month for this
    # file tag. This means that this file tag or table itself likely has some sort of
    # audit column that allows the state to generate date-bounded diffs.
    ONE_MONTH_INCREMENTAL_LOOKBACK = "ONE_MONTH_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to ONLY include the last two weeks worth of data for this
    # file tag. This means that this file tag or table itself likely has some sort of
    # audit column that allows the state to generate date-bounded diffs.
    TWO_WEEK_INCREMENTAL_LOOKBACK = "TWO_WEEK_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to ONLY include the last weeks worth of data for this file
    # tag. This means that this file tag or table itself likely has some sort of audit
    # column that allows the state to generate date-bounded diffs.
    ONE_WEEK_INCREMENTAL_LOOKBACK = "ONE_WEEK_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to only include recent data for this file tag, but
    # we're not exactly sure how much historical data is included with each file. This
    # means that this file tag or table itself likely has some sort of audit column that
    # allows the state to generate date-bounded diffs.
    UNKNOWN_INCREMENTAL_LOOKBACK = "UNKNOWN_INCREMENTAL_LOOKBACK"


class ColumnUpdateOperation(Enum):
    """Enum for column update operations"""

    ADDITION = "ADDITION"
    DELETION = "DELETION"
    RENAME = "RENAME"
