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
"""Helpers for environment detection"""
import os
from typing import Optional

# Environment variable that returns the name of the compsoer environment
# https://cloud.google.com/composer/docs/how-to/managing/environment-variables
COMPOSER_ENVIRONMENT = "COMPOSER_ENVIRONMENT"


def get_composer_environment() -> Optional[str]:
    return os.getenv(COMPOSER_ENVIRONMENT)


def is_experiment_environment() -> bool:
    composer_environment = get_composer_environment()
    return composer_environment is not None and "experiment" in composer_environment


def get_project_id() -> str:
    """
    Returns the project ID for the current environment.
    """
    if not (project_id_opt := os.environ.get("GCP_PROJECT")):
        raise ValueError("environment variable GCP_PROJECT not set.")

    return project_id_opt
