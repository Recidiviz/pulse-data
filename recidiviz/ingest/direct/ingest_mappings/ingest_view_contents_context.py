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
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.feature_flags_registry import resolve_ingest_feature_flags
from recidiviz.ingest.direct.ingest_mappings.env_property_utils import (
    IS_LOCAL_PROPERTY_NAME,
    IS_PRODUCTION_PROPERTY_NAME,
    IS_STAGING_PROPERTY_NAME,
    feature_flag_for_env_property,
)
from recidiviz.utils import environment
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class IngestViewContentsContext:
    """
    This class is used to gate parsing logic for parsing ingest view results by checking
    the '$env: value' portion of an ingest mapping against local, staging, or production.

    TODO(#37799) Update the use of this to only work with `should_launch`, rather than generally.
    """

    def __init__(
        self,
        is_local: bool,
        is_staging: bool,
        is_production: bool,
        is_sandbox: bool,
        state_code: StateCode,
        # Value of every registered ingest feature flag, keyed by the bare
        # (unprefixed) flag name. Already resolved for the relevant project,
        # so that this context stays a simple bundle of static values.
        feature_flags: dict[str, bool],
    ) -> None:
        self.is_local = is_local
        self.is_staging = is_staging
        self.is_production = is_production
        self.is_sandbox = is_sandbox
        self.state_code = state_code
        self.feature_flags = feature_flags

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
        if (flag_name := feature_flag_for_env_property(property_name)) is not None:
            if flag_name not in self.feature_flags:
                raise ValueError(
                    f"Unregistered feature flag [{flag_name}] referenced via `$env` "
                    f"property [{property_name}]. Register it in "
                    f"recidiviz/ingest/direct/feature_flags_registry.py."
                )
            return self.feature_flags[flag_name]

        raise ValueError(f"Unexpected environment property: [{property_name}]")

    @classmethod
    @environment.test_only
    def build_for_tests(
        cls,
        state_code: StateCode,
        project_id: str = GCP_PROJECT_STAGING,
    ) -> "IngestViewContentsContext":
        """Creates a context for use in tests. Ingest views gated with `is_local: True`
        will be run with this context.

        Feature flags are resolved for `project_id`, which defaults to staging
        to match the staging-like posture of the other properties here. Pass
        GCP_PROJECT_PRODUCTION to exercise the production side of a flag.
        """
        return IngestViewContentsContext(
            is_local=True,
            # We run all views gated to staging in tests
            is_staging=True,
            is_production=False,
            is_sandbox=False,
            state_code=state_code,
            feature_flags=resolve_ingest_feature_flags(project_id),
        )

    @classmethod
    def build_for_project(
        cls,
        project_id: str,
        is_sandbox: bool,
        state_code: StateCode,
    ) -> "IngestViewContentsContext":
        """Creates context for an ingest view that will be processed in an ingest
        pipeline running in the given project.
        """
        return IngestViewContentsContext(
            is_local=False,
            is_staging=project_id == GCP_PROJECT_STAGING,
            is_production=project_id == GCP_PROJECT_PRODUCTION,
            is_sandbox=is_sandbox,
            state_code=state_code,
            feature_flags=resolve_ingest_feature_flags(project_id),
        )
