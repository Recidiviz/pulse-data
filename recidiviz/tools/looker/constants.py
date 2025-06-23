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
"""Constants for Looker generation scripts."""
import os

import recidiviz

LOOKER_REPO_NAME = "looker"

VIEWS_DIR = "views"
DASHBOARDS_DIR = "dashboards"
EXPLORES_DIR = "explores"

RECIDIVIZ_ROOT = os.path.dirname(recidiviz.__file__)
LOOKER_TOOLS_ROOT = "tools/looker"

GENERATED_SUBDIR_NAME = "generated"
GENERATED_VERSION_FILE_NAME = "generated_version_hash"

GENERATED_LOOKML_ROOT_PATH = os.path.join(
    RECIDIVIZ_ROOT, LOOKER_TOOLS_ROOT, GENERATED_SUBDIR_NAME
)
GENERATED_VERSION_FILE_PATH = os.path.join(
    RECIDIVIZ_ROOT, LOOKER_TOOLS_ROOT, GENERATED_VERSION_FILE_NAME
)
