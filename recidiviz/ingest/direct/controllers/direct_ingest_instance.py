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
# =============================================================================
"""Defines enum for specifying an independent set of ingest data / infrastructure for a
given region.
"""
from enum import Enum
from typing import Optional

from recidiviz.cloud_functions.direct_ingest_bucket_name_utils import (
    is_primary_ingest_bucket,
    is_secondary_ingest_bucket,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.errors import DirectIngestInstanceError
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyStateDatabaseVersion,
)


class DirectIngestInstance(Enum):
    """Enum for specifying an independent set of ingest data / infrastructure for a
    given region.
    """

    # Ingest instance whose ingested data is exported to BQ and may be shipped to
    # products.
    PRIMARY = "PRIMARY"

    # Ingest instance that may be used for background ingest operations, such as a full
    # rerun.
    SECONDARY = "SECONDARY"

    def check_is_valid_system_level(self, system_level: SystemLevel) -> None:
        """Throws a DirectIngestInstanceError if this is not a valid instance for the
        given system level.
        """
        if system_level == SystemLevel.COUNTY:
            if self != self.PRIMARY:
                raise DirectIngestInstanceError(
                    f"Direct ingest for [{system_level}] only has single, "
                    f"primary ingest instance. Ingest instance [{self}] not valid."
                )

    def database_version(
        # TODO(#7984): Remove the state_code arg once all states have been migrated to
        #   multi-DB.
        self,
        system_level: SystemLevel,
        state_code: Optional[StateCode],
    ) -> SQLAlchemyStateDatabaseVersion:
        """Return the database version for this instance."""
        self.check_is_valid_system_level(system_level)

        if system_level == SystemLevel.COUNTY:
            # County direct ingest writes to single, multi-tenant database
            return SQLAlchemyStateDatabaseVersion.LEGACY

        if system_level == SystemLevel.STATE:
            if not state_code:
                raise ValueError("Found null state_code for STATE schema.")
            if self == self.SECONDARY:
                return SQLAlchemyStateDatabaseVersion.SECONDARY
            if self == self.PRIMARY:
                return SQLAlchemyStateDatabaseVersion.PRIMARY

        raise ValueError(
            f"Unexpected combination of [{system_level}] and instance type [{self}]"
        )

    @classmethod
    def for_ingest_bucket(
        cls, ingest_bucket: GcsfsBucketPath
    ) -> "DirectIngestInstance":
        if is_primary_ingest_bucket(ingest_bucket.bucket_name):
            return cls.PRIMARY
        if is_secondary_ingest_bucket(ingest_bucket.bucket_name):
            return cls.SECONDARY
        raise ValueError(f"Unexpected ingest bucket [{ingest_bucket.bucket_name}]")

    @classmethod
    def for_state_database_version(
        cls,
        database_version: SQLAlchemyStateDatabaseVersion,
        state_code: StateCode,
    ) -> "DirectIngestInstance":
        if database_version == SQLAlchemyStateDatabaseVersion.SECONDARY:
            return cls.SECONDARY
        if database_version in (
            SQLAlchemyStateDatabaseVersion.PRIMARY,
            SQLAlchemyStateDatabaseVersion.LEGACY,
        ):
            expected_primary_db_version = cls.PRIMARY.database_version(
                SystemLevel.STATE, state_code
            )
            # TODO(#7984): Remove this check once there are no states running ingest out
            #   of a LEGACY DB.
            if expected_primary_db_version != database_version:
                raise ValueError(
                    f"Requested database version [{database_version}] is not valid for state [{state_code}]."
                )
            return cls.PRIMARY
        raise ValueError(f"Unexpected database version [{database_version}]")
