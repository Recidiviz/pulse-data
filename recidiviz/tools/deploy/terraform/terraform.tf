# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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

terraform {
  # Note: this verison number should be kept in sync with the ones in Dockerfile,
  # .devcontainer/devcontainer.json, .github/workflows/ci.yml, and
  # recidiviz/tools/deploy/deploy_helpers.sh
  # NOTE: The ~> operator means 'Greater than or equal to this version but less than the
  # next minor version. E.g. '~> 1.11.4' means greater than or equal to 1.11.4 but less
  # than 1.12 (1.11.5 would be allowed).
  required_version = "~> 1.11.4"
}
