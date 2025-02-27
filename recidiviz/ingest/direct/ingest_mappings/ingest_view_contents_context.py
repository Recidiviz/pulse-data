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
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IS_LOCAL_PROPERTY_NAME,
    IS_PRODUCTION_PROPERTY_NAME,
    IS_STAGING_PROPERTY_NAME,
)
from recidiviz.utils import environment
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class IngestViewContentsContext:
    """
    This class is used to gate parsing logic for parsing ingest view results by checking
    the '$env: value' portion of an ingest mapping against local, staging, or production.

    TODO(#37799) Update the use of this to only work with `should_launch`, rather than generally.
    """

    def __init__(self, is_local: bool, is_staging: bool, is_production: bool) -> None:
        self.is_local = is_local
        self.is_staging = is_staging
        self.is_production = is_production

    def get_env_property(self, property_name: str) -> bool:
        """
        Returns a value associated with an environment or other metadata property
        associated with this parsing job.
        Throws ValueError for all unexpected property names.
        """
        if property_name == IS_LOCAL_PROPERTY_NAME:
            return self.is_local
        if property_name == IS_STAGING_PROPERTY_NAME:
            return self.is_staging
        if property_name == IS_PRODUCTION_PROPERTY_NAME:
            return self.is_production

        raise ValueError(f"Unexpected environment property: [{property_name}]")

    @classmethod
    @environment.test_only
    def build_for_tests(cls) -> "IngestViewContentsContext":
        """Creates a context for use in tests. Ingest views gated with `is_local: True`
        will be run with this context"""
        return IngestViewContentsContext(
            is_local=True,
            # We run all views gated to staging in tests
            is_staging=True,
            is_production=False,
        )

    @classmethod
    def build_for_project(cls, project_id: str) -> "IngestViewContentsContext":
        """Creates context for an ingest view that will be processed in an ingest
        pipeline running in the given project.
        """
        return IngestViewContentsContext(
            is_local=False,
            is_staging=project_id == GCP_PROJECT_STAGING,
            is_production=project_id == GCP_PROJECT_PRODUCTION,
        )
