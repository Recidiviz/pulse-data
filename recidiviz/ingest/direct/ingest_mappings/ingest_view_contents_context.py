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
from typing import Union

from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IS_LOCAL_PROPERTY_NAME,
    IS_PRODUCTION_PROPERTY_NAME,
    IS_STAGING_PROPERTY_NAME,
)
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

    def get_env_property(self, property_name: str) -> Union[bool, str]:
        if property_name == IS_LOCAL_PROPERTY_NAME:
            return not environment.in_gcp()
        if property_name == IS_STAGING_PROPERTY_NAME:
            return environment.in_gcp_staging()
        if property_name == IS_PRODUCTION_PROPERTY_NAME:
            return environment.in_gcp_production()

        raise ValueError(f"Unexpected environment property: [{property_name}]")
