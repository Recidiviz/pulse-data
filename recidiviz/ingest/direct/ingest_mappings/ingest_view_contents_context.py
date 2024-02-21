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
"""Interface and implementation for a class that abstracts state/schema specific
logic from the IngestViewManifest. This class may contain context that differs
between rows in the ingest view results.
"""
import abc
import datetime
from typing import Optional, Union

from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    INGEST_VIEW_RESULTS_UPDATE_DATETIME,
    IS_LOCAL_PROPERTY_NAME,
    IS_PRIMARY_INSTANCE_PROPERTY_NAME,
    IS_PRODUCTION_PROPERTY_NAME,
    IS_SECONDARY_INSTANCE_PROPERTY_NAME,
    IS_STAGING_PROPERTY_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import environment


class IngestViewContentsContext:
    """Interface for a class that abstracts state/schema specific logic from the
    IngestViewManifest. This class may contain context that differs between rows in the
    ingest view results.
    """

    @abc.abstractmethod
    def get_env_property(self, property_name: str) -> Union[bool, str]:
        """Returns a value associated with an environment or other metadata property
        associated with this parsing job. Throws ValueError for all unexpected
        property names.
        """


class IngestViewContentsContextImpl(IngestViewContentsContext):
    """Standard implementation of the IngestViewContentsContext, for use in
    production code.
    """

    def __init__(
        self,
        ingest_instance: DirectIngestInstance,
        results_update_datetime: Optional[datetime.datetime] = None,
    ) -> None:
        self.ingest_instance = ingest_instance
        self.results_update_datetime = results_update_datetime

    def get_env_property(self, property_name: str) -> Union[bool, str]:
        if property_name == IS_LOCAL_PROPERTY_NAME:
            return not environment.in_gcp()
        if property_name == IS_STAGING_PROPERTY_NAME:
            return environment.in_gcp_staging()
        if property_name == IS_PRODUCTION_PROPERTY_NAME:
            return environment.in_gcp_production()
        if property_name == IS_PRIMARY_INSTANCE_PROPERTY_NAME:
            return self.ingest_instance == DirectIngestInstance.PRIMARY
        if property_name == IS_SECONDARY_INSTANCE_PROPERTY_NAME:
            return self.ingest_instance == DirectIngestInstance.SECONDARY
        if property_name == INGEST_VIEW_RESULTS_UPDATE_DATETIME:
            if self.results_update_datetime is None:
                raise ValueError(
                    "The results_update_datetime is not defined for this context - "
                    "cannot use this to parse a manifest that uses an "
                    "`$env: results_update_datetime` clause."
                )
            results_update_datetime_str = self.results_update_datetime.isoformat()
            return results_update_datetime_str

        raise ValueError(f"Unexpected environment property: [{property_name}]")
