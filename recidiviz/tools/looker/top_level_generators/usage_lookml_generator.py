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
"""
Generates LookML files for usage views.
"""
import os

from recidiviz.tools.looker.script_helpers import remove_lookml_files_from
from recidiviz.tools.looker.top_level_generators.custom_metrics_lookml_generator import (
    LookMLGenerator,
)
from recidiviz.tools.looker.usage_views.global_provisioned_user_sessions_view_generator import (
    build_global_provisioned_user_sessions_lookml_view,
)


class UsageLookMLGenerator(LookMLGenerator):
    @classmethod
    def generate_lookml(cls, output_dir: str) -> None:
        """Generate LookML files for usage views."""
        usage_views_dir = os.path.join(output_dir, "views", "usage_views")
        remove_lookml_files_from(usage_views_dir)
        os.makedirs(usage_views_dir, exist_ok=True)
        view = build_global_provisioned_user_sessions_lookml_view()
        view.write(usage_views_dir, source_script_path=__file__)
