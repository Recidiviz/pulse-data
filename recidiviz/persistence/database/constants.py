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
"""Constants for interacting with the database"""

SQLALCHEMY_DB_NAME = "SQLALCHEMY_DB_NAME"
SQLALCHEMY_DB_HOST = "SQLALCHEMY_DB_HOST"
SQLALCHEMY_DB_PORT = "SQLALCHEMY_DB_PORT"
SQLALCHEMY_DB_USER = "SQLALCHEMY_DB_USER"
SQLALCHEMY_DB_PASSWORD = "SQLALCHEMY_DB_PASSWORD"

# We have two sets of Justice Counts DB instances -- one original set in the
# Recidiviz GCP projects and one new set in the Justice Counts GCP projects.
# The secrets for the original instance are prefixed with `justice_counts`,
# and the secrets for the new instance are prefixed with `justice_counts_v2`.
# TODO(#23253): Remove when Publisher is migrated to JC GCP project. Then
# the Recidiviz GCP project will have one set of secrets that point to the
# original JC instance, and the JC GCP projects will have one set of secrets
# that point to the new JC instance.
JUSTICE_COUNTS_DB_SECRET_PREFIX = "justice_counts_v2"  # nosec
