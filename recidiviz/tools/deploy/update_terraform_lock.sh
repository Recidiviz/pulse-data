#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

# The terraform lock file stores a series of hashes it uses to know which version of a provider has
# most recently been used in this repository. The file is read/written during `terraform init`,
# and can change for one of several reasons, including:
# 1. There's a new version of Terraform that changed the hashing algorithm or output
# 2. A provider is added, upgraded, or removed
# 3. A user runs Terraform from a new platform
# We want this file to change at the time of a commit to the Terraform directory rather than waiting
# until `terraform init` is run so that changes are checked in prior to deploys (which fail if there
# are uncommitted changes).
# Generally, this script should not need to be run outside of pre-commits, though may rarely need to
# be if someone installs a version of Terraform that changed the contents of the file without making
# other code changes.

terraform -chdir=recidiviz/tools/deploy/terraform providers lock \
  -platform=darwin_amd64 \
  -platform=darwin_arm64
