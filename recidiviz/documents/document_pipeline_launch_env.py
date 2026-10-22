# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""The launch tier of a document collection or extractor: where it is allowed to
run.
"""

from enum import Enum

from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION


class DocumentPipelineLaunchEnv(Enum):
    """Where a document collection or extractor is allowed to run. Tiers are
    cumulative: STAGING includes local runs; PRODUCTION includes staging and
    local runs. Local/sandbox tooling may run any config regardless of tier.
    """

    # NOTE: These are listed from narrowest to broadest tier, and rank below
    #  depends on that order. DO NOT CHANGE THE ORDER OF THESE ENUMS RELATIVE TO
    #  EACH OTHER.
    LOCAL_ONLY = "LOCAL_ONLY"
    STAGING = "STAGING"
    PRODUCTION = "PRODUCTION"

    @property
    def rank(self) -> int:
        """Returns this tier's position in the cumulative ordering (0 =
        narrowest).
        """
        return list(DocumentPipelineLaunchEnv).index(self)

    def is_at_least(self, other: "DocumentPipelineLaunchEnv") -> bool:
        """Returns whether this tier is as broad as or broader than the other
        tier.
        """
        return self.rank >= other.rank

    def is_launched_in_project(self, project_id: str) -> bool:
        """Returns whether a config with this tier runs in the given project."""
        return self.is_at_least(_minimum_launch_env_for_project(project_id))


def _minimum_launch_env_for_project(project_id: str) -> DocumentPipelineLaunchEnv:
    """Returns the narrowest tier a config must have to run in the given project.
    Any non-production project (staging, test projects) runs staging-tier configs.
    """
    if project_id == GCP_PROJECT_PRODUCTION:
        return DocumentPipelineLaunchEnv.PRODUCTION
    return DocumentPipelineLaunchEnv.STAGING
